/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.memory

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.util.{LongAccumulator, Utils}

/**
 * Implements policies and bookkeeping for sharing an adjustable-sized pool of memory between tasks.
 *
 * Tries to ensure that each task gets a reasonable share of memory, instead of some task ramping up
 * to a large amount first and then causing others to spill to disk repeatedly.
 *
 * If there are N tasks, it ensures that each task can acquire at least 1 / 2N of the memory
 * before it has to spill, and at most 1 / N. Because N varies dynamically, we keep track of the
 * set of active tasks and redo the calculations of 1 / 2N and 1 / N in waiting tasks whenever this
 * set changes. This is all done by synchronizing access to mutable state and using wait() and
 * notifyAll() to signal changes to callers. Prior to Spark 1.6, this arbitration of memory across
 * tasks was performed by the ShuffleMemoryManager.
 *
 * @param lock a [[MemoryManager]] instance to synchronize on
 * @param memoryMode the type of memory tracked by this pool (on- or off-heap)
 */
private[memory] class ExecutionMemoryPool(
    lock: Object,
    memoryMode: MemoryMode
  ) extends MemoryPool(lock) with Logging {

  private[this] val poolName: String = memoryMode match {
    case MemoryMode.ON_HEAP => "on-heap execution"
    case MemoryMode.OFF_HEAP => "off-heap execution"
  }
  /**
   * Map from taskAttemptId -> memory consumption in bytes
   */
  @GuardedBy("lock")
  private val memoryForTask = new mutable.HashMap[Long, Long]()

  var memoryForUnsafeExternalSorter = new LongAccumulator
  var memoryForShuffleExternalSorter = new LongAccumulator
  var memoryForVariableLengthRowBasedKeyValueBatch = new LongAccumulator
  var memoryForBytesToBytesMap = new LongAccumulator
  var memoryForExternalAppendOnlyMap = new LongAccumulator
  var memoryForLongToUnsafeRowMap = new LongAccumulator
  var memoryForExternalSorter = new LongAccumulator
  var memoryForFixedLengthRowBasedKeyValueBatch = new LongAccumulator

  @GuardedBy("lock")
  private val memoryconsumersForTask = new mutable.HashMap[Long, List[String]]()

  override def memoryUsed: Long = lock.synchronized {
    memoryForTask.values.sum
  }

  def getTotalMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }

  def getMemoryConsumersForTask(taskAttemptId: Long): List[String] = lock.synchronized {
    memoryconsumersForTask.getOrElse(taskAttemptId, Nil)
  }

  def removeTrackerForTask(taskAttemptId: Long): Unit = lock.synchronized {
    memoryForTask.remove(taskAttemptId)
    memoryconsumersForTask.remove(taskAttemptId)
  }
  /**
   * Returns the memory consumption, in bytes, for the given task.
   */
  def getMemoryUsageForTask(taskAttemptId: Long): Long = lock.synchronized {
    memoryForTask.getOrElse(taskAttemptId, 0L)
  }
  def updateMemoryRequestForConsumer(mc: String, size: Long): Unit =
  {
    mc match {
      case "ShuffleExternalSorter" =>
        memoryForShuffleExternalSorter.add(size)
      case "BytesToBytesMap" =>
        memoryForBytesToBytesMap.add(size)
      case "VariableLengthRowBasedKeyValueBatch" =>
        memoryForVariableLengthRowBasedKeyValueBatch.add(size)
      case "UnsafeExternalSorter" =>
        memoryForUnsafeExternalSorter.add(size)
      case "ExternalAppendOnlyMap" =>
        memoryForExternalAppendOnlyMap.add(size)
      case "ExternalSorter" =>
        memoryForExternalSorter.add(size)
      case "LongToUnsafeRowMap" =>
        memoryForLongToUnsafeRowMap.add(size)
      case "FixedLengthRowBasedKeyValueBatch" =>
        memoryForFixedLengthRowBasedKeyValueBatch.add(size)
      case _ => logInfo(s"${mc} doesn't support now.")
    }
  }

  def getMemoryRequestForConsumer(mc: String): Long = lock.synchronized{
    mc match {
      case "ShuffleExternalSorter" => memoryForShuffleExternalSorter.value
      case "UnsafeExternalSorter" => memoryForUnsafeExternalSorter.value
      case "VariableLengthRowBasedKeyValueBatch" =>
        memoryForVariableLengthRowBasedKeyValueBatch.value
      case "FixedLengthRowBasedKeyValueBatch" =>
        memoryForFixedLengthRowBasedKeyValueBatch.value
      case "BytesToBytesMap" => memoryForBytesToBytesMap.value
      case "ExternalAppendOnlyMap" => memoryForExternalAppendOnlyMap.value
      case "LongToUnsafeRowMap" => memoryForLongToUnsafeRowMap.value
      case "ExternalSorter" => memoryForExternalSorter.value
      case _ =>
        logInfo(s"${mc} doesn't support now.")
        0L
    }
  }
  /**
   * Try to acquire up to `numBytes` of memory for the given task and return the number of bytes
   * obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   *
   * @param numBytes number of bytes to acquire
   * @param taskAttemptId the task attempt acquiring memory
   * @param maybeGrowPool a callback that potentially grows the size of this pool. It takes in
   *                      one parameter (Long) that represents the desired amount of memory by
   *                      which this pool should be expanded.
   * @param computeMaxPoolSize a callback that returns the maximum allowable size of this pool
   *                           at this given moment. This is not a field because the max pool
   *                           size is variable in certain cases. For instance, in unified
   *                           memory management, the execution pool can be expanded by evicting
   *                           cached blocks, thereby shrinking the storage pool.
   *
   * @return the number of bytes granted to the task.
   */
  private[memory] def acquireMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryConsumer: MemoryConsumer,
      maybeGrowPool: Long => Unit = (additionalSpaceNeeded: Long) => (),
      computeMaxPoolSize: () => Long = () => poolSize): Long = lock.synchronized {
    assert(numBytes > 0, s"invalid number of bytes requested: $numBytes")

    // TODO: clean up this clunky method signature

    // Add this task to the taskMemory map just so we can keep an accurate count of the number
    // of active tasks, to let other tasks ramp down their memory in calls to `acquireMemory`
    if (!memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) = 0L
      // This will later cause waiting tasks to wake up and check numTasks again
      lock.notifyAll()
    }

    if(!memoryconsumersForTask.contains(taskAttemptId)) {
      memoryconsumersForTask(taskAttemptId) = Nil
      lock.notifyAll()
    }
    // Keep looping until we're either sure that we don't want to grant this request (because this
    // task would have more than 1 / numActiveTasks of the memory) or we have enough free
    // memory to give it (we always let each task get at least 1 / (2 * numActiveTasks)).
    // TODO: simplify this to limit each task to its own slot
    while (true) {
      val numActiveTasks = memoryForTask.keys.size
      val curMem = memoryForTask(taskAttemptId)

      // In every iteration of this loop, we should first try to reclaim any borrowed execution
      // space from storage. This is necessary because of the potential race condition where new
      // storage blocks may steal the free execution memory that this task was waiting for.
      maybeGrowPool(numBytes - memoryFree)

      // Maximum size the pool would have after potentially growing the pool.
      // This is used to compute the upper bound of how much memory each task can occupy. This
      // must take into account potential free memory as well as the amount this pool currently
      // occupies. Otherwise, we may run into SPARK-12155 where, in unified memory management,
      // we did not take into account space that could have been freed by evicting cached blocks.
      val maxPoolSize = computeMaxPoolSize()
      val maxMemoryPerTask = maxPoolSize / numActiveTasks
      val minMemoryPerTask = poolSize / (2 * numActiveTasks)

      // How much we can grant this task; keep its share within 0 <= X <= 1 / numActiveTasks
      val maxToGrant = math.min(numBytes, math.max(0, maxMemoryPerTask - curMem))
      // Only give it as much memory as is free, which might be none if it reached 1 / numTasks
      val toGrant = math.min(maxToGrant, memoryFree)

      // We want to let each task get at least 1 / (2 * numActiveTasks) before blocking;
      // if we can't give it this much now, wait for other tasks to free up memory
      // (this happens if older tasks allocated lots of memory before N grew)
      if (toGrant < numBytes && curMem + toGrant < minMemoryPerTask) {
        logInfo(s"TID $taskAttemptId waiting for at least 1/2N of $poolName pool to be free")
        lock.wait()
      } else {
        val memoryConsumerType =
          memoryConsumer.toString.substring(0, memoryConsumer.toString().indexOf('@'))
        val mc = memoryConsumerType.split('.')
        val consumer = mc(mc.length - 1)
        if(!memoryconsumersForTask(taskAttemptId).contains(consumer)) {
          val mcs = consumer +: memoryconsumersForTask(taskAttemptId)
          memoryconsumersForTask(taskAttemptId) = mcs
        }
        updateMemoryRequestForConsumer(consumer, toGrant)
        memoryForTask(taskAttemptId) += toGrant
        return toGrant
      }
    }
    0L  // Never reached
  }

  /**
   * Release `numBytes` of memory acquired by the given task.
   */
  def releaseMemory(numBytes: Long, taskAttemptId: Long): Unit = lock.synchronized {
    val curMem = memoryForTask.getOrElse(taskAttemptId, 0L)
    val memoryToFree = if (curMem < numBytes) {
      logWarning(
        s"Internal error: release called on $numBytes bytes but task only has $curMem bytes " +
          s"of memory from the $poolName pool")
      curMem
    } else {
      numBytes
    }
    if (memoryForTask.contains(taskAttemptId)) {
      memoryForTask(taskAttemptId) -= memoryToFree
      if (memoryForTask(taskAttemptId) <= 0) {
        memoryForTask.remove(taskAttemptId)
      }
    }
    lock.notifyAll() // Notify waiters in acquireMemory() that memory has been freed
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   * @return the number of bytes freed.
   */
  def releaseAllMemoryForTask(taskAttemptId: Long): Long = lock.synchronized {
    val numBytesToFree = getMemoryUsageForTask(taskAttemptId)
    releaseMemory(numBytesToFree, taskAttemptId)
    numBytesToFree
  }

}
