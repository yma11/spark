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
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.storage.BlockId
import org.apache.spark.storage.memory.MemoryStore
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.memory.MemoryAllocator
import org.apache.spark.util.{LongAccumulator, Utils}

import scala.collection.mutable

/**
 * An abstract memory manager that enforces how memory is shared between execution and storage.
 *
 * In this context, execution memory refers to that used for computation in shuffles, joins,
 * sorts and aggregations, while storage memory refers to that used for caching and propagating
 * internal data across the cluster. There exists one MemoryManager per JVM.
 */
private[spark] abstract class MemoryManager(
    conf: SparkConf,
    numCores: Int,
    onHeapStorageMemory: Long,
    onHeapExecutionMemory: Long) extends Logging {

  require(onHeapExecutionMemory > 0, "onHeapExecutionMemory must be > 0")

  // -- Methods related to memory allocation policies and bookkeeping ------------------------------

  @GuardedBy("this")
  protected val onHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapStorageMemoryPool = new StorageMemoryPool(this, MemoryMode.OFF_HEAP)
  @GuardedBy("this")
  protected val onHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.ON_HEAP)
  @GuardedBy("this")
  protected val offHeapExecutionMemoryPool = new ExecutionMemoryPool(this, MemoryMode.OFF_HEAP)

  onHeapStorageMemoryPool.incrementPoolSize(onHeapStorageMemory)
  onHeapExecutionMemoryPool.incrementPoolSize(onHeapExecutionMemory)

  protected[this] val maxOffHeapMemory = conf.get(MEMORY_OFFHEAP_SIZE)
  protected[this] val offHeapStorageMemory =
    (maxOffHeapMemory * conf.get(MEMORY_STORAGE_FRACTION)).toLong

  offHeapExecutionMemoryPool.incrementPoolSize(maxOffHeapMemory - offHeapStorageMemory)
  offHeapStorageMemoryPool.incrementPoolSize(offHeapStorageMemory)

  /**
   * Total available on heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   * In this model, this is equivalent to the amount of memory not occupied by execution.
   */
  def maxOnHeapStorageMemory: Long

  /**
   * Total available off heap memory for storage, in bytes. This amount can vary over time,
   * depending on the MemoryManager implementation.
   */
  def maxOffHeapStorageMemory: Long

  // track spill size of each consumer
  private[memory] var _spilledForMemoryConsumer = new mutable.HashMap[String, Long]()
  private[memory] def spilledForMemoryConsumer: mutable.HashMap[String, Long] = {
    _spilledForMemoryConsumer
  }

  private[memory] def trackSpilledForMemoryConsumer(memoryConsumer: MemoryConsumer,
                                                    size: Long): Unit

  private[memory] def trackSpilledForMemoryConsumer(memoryConsumer: String,
                                                    size: Long): Unit

  // track total memory spill size
  private[memory] val _totalMemorySpilledSize = new LongAccumulator

  private[memory] def incMemorySpillSize(v: Long): Unit

  private[memory] def totalMemorySpilledSize: Long = {
    _totalMemorySpilledSize.value
  }

  // track total disk spill size
  private[memory] val _totalDiskSpilledSize = new LongAccumulator
  private[memory] def incDiskSpillSize(v: Long): Unit
  private[memory] def totalDiskSpilledSize: Long = {
    _totalDiskSpilledSize.value
  }

  // track peak memory of current task
  private[memory] var _peakMemoryUsedForTask = 0L
  private[memory] var _taskWithPeakMemory = 0L
  def peakMemoryUsedForTask: Long = _peakMemoryUsedForTask

  // track totalMemoryBorrowedFromExecution
  private[memory] val _totalMemoryBorrowedFromExecution = new LongAccumulator
  private[memory] def totalMemoryBorrowedFromExecution: Long = {
    _totalMemoryBorrowedFromExecution.value
  }
  private[memory] def incTotalMemoryBorrowedFromExecution(v: Long): Unit

  // track totalMemoryBorrowedFromStorage
  private[memory] val _totalMemoryBorrowedFromStorage = new LongAccumulator
  private[memory] def totalMemoryBorrowedFromStorage: Long = {
    _totalMemoryBorrowedFromStorage.value
  }
  private[memory] def incTotalMemoryBorrowedFromStorage(v: Long): Unit
  /**
   * Set the [[MemoryStore]] used by this manager to evict cached blocks.
   * This must be set after construction due to initialization ordering constraints.
   */
  final def setMemoryStore(store: MemoryStore): Unit = synchronized {
    onHeapStorageMemoryPool.setMemoryStore(store)
    offHeapStorageMemoryPool.setMemoryStore(store)
  }

  /**
   * Acquire N bytes of memory to cache the given block, evicting existing ones if necessary.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireStorageMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Acquire N bytes of memory to unroll the given block, evicting existing ones if necessary.
   *
   * This extra method allows subclasses to differentiate behavior between acquiring storage
   * memory and acquiring unroll memory. For instance, the memory management model in Spark
   * 1.5 and before places a limit on the amount of space that can be freed from unrolling.
   *
   * @return whether all N bytes were successfully granted.
   */
  def acquireUnrollMemory(blockId: BlockId, numBytes: Long, memoryMode: MemoryMode): Boolean

  /**
   * Try to acquire up to `numBytes` of execution memory for the current task and return the
   * number of bytes obtained, or 0 if none can be allocated.
   *
   * This call may block until there is enough free memory in some situations, to make sure each
   * task has a chance to ramp up to at least 1 / 2N of the total memory pool (where N is the # of
   * active tasks) before it is forced to spill. This can happen if the number of tasks increase
   * but an older task had a lot of memory already.
   */
  private[memory]
  def acquireExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryConsumer: MemoryConsumer,
      memoryMode: MemoryMode): Long

  /**
   * Release numBytes of execution memory belonging to the given task.
   */
  private[memory]
  def releaseExecutionMemory(
      numBytes: Long,
      taskAttemptId: Long,
      memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.releaseMemory(numBytes, taskAttemptId)
    }
  }

  /**
   * Release all memory for the given task and mark it as inactive (e.g. when a task ends).
   *
   * @return the number of bytes freed.
   */
  private[memory] def releaseAllExecutionMemoryForTask(taskAttemptId: Long): Long = synchronized {
    if (conf.get(SPARK_MEMORY_SPILL_LOG)) {
      var spilledInfo: String = ""
      for(mc <- _spilledForMemoryConsumer.keys) {
        spilledInfo += ", " + mc + " spills " + Utils.bytesToString(_spilledForMemoryConsumer(mc))
      }
      if(spilledInfo != "") {
        logInfo(s"till now total memory/disk spilled size is " +
          s"${Utils.bytesToString(_totalMemorySpilledSize.value)}" +
          s"/${Utils.bytesToString(_totalDiskSpilledSize.value)}" +
          s" ${spilledInfo}")
      }
      logInfo(s"till now totalMemoryBorrowedFromExecution is " +
        s"${Utils.bytesToString(totalMemoryBorrowedFromExecution)}")
      logInfo(s"till now totalMemoryBorrowedFromStorage is " +
        s"${Utils.bytesToString(totalMemoryBorrowedFromStorage)}")
      val totalDropFromStroageMemory =
        offHeapStorageMemoryPool.evictedStorage + onHeapStorageMemoryPool.evictedStorage
      if (totalDropFromStroageMemory != 0) {
        logInfo(s"till now totalDropFromStroageMemory is " +
          s"${Utils.bytesToString(totalDropFromStroageMemory)}")
      }
      val memroyForTask = onHeapExecutionMemoryPool.getTotalMemoryUsageForTask(taskAttemptId) +
        offHeapExecutionMemoryPool.getTotalMemoryUsageForTask(taskAttemptId)
      // + aepExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
      var isRemovable = true
      if(memroyForTask > _peakMemoryUsedForTask) {
        _peakMemoryUsedForTask = memroyForTask
        _taskWithPeakMemory = taskAttemptId
        isRemovable = false
      }
      val consumers = onHeapExecutionMemoryPool.
        getMemoryConsumersForTask(_taskWithPeakMemory):::offHeapExecutionMemoryPool.
        getMemoryConsumersForTask(_taskWithPeakMemory)
      logInfo(s"till now peakmemoryusedbytask is " + Utils.bytesToString(_peakMemoryUsedForTask)
        + "(" + consumers.mkString(",") + ")")
      // remove tracker for current task or the hashmap becomes too large
      // when amount of tasks executed
      if(isRemovable) {
        onHeapExecutionMemoryPool.removeTrackerForTask(taskAttemptId)
        offHeapExecutionMemoryPool.removeTrackerForTask(taskAttemptId)
      }
      // print memory request metrics
      var requestStats = ""
      val memoryConsumerList: List[String] = List("ShuffleExternalSorter", "UnsafeExternalSorter",
        "VariableLengthRowBasedKeyValueBatch", "BytesToBytesMap", "ExternalAppendOnlyMap",
        "LongToUnsafeRowMap", "ExternalSorter", "FixedLengthRowBasedKeyValueBatch")
      for(mc <- memoryConsumerList) {
        val requestSize = onHeapExecutionMemoryPool.getMemoryRequestForConsumer(mc) +
          offHeapExecutionMemoryPool.getMemoryRequestForConsumer(mc)
        if(requestSize > 0) {
          requestStats += mc + " requests total " + Utils.bytesToString(requestSize) + "."
        }
      }
      if(requestStats != "") {
        logInfo(requestStats)
      }
    }
    onHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.releaseAllMemoryForTask(taskAttemptId)
  }

  /**
   * Release N bytes of storage memory.
   */
  def releaseStorageMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    memoryMode match {
      case MemoryMode.ON_HEAP => onHeapStorageMemoryPool.releaseMemory(numBytes)
      case MemoryMode.OFF_HEAP => offHeapStorageMemoryPool.releaseMemory(numBytes)
    }
  }

  /**
   * Release all storage memory acquired.
   */
  final def releaseAllStorageMemory(): Unit = synchronized {
    onHeapStorageMemoryPool.releaseAllMemory()
    offHeapStorageMemoryPool.releaseAllMemory()
  }

  /**
   * Release N bytes of unroll memory.
   */
  final def releaseUnrollMemory(numBytes: Long, memoryMode: MemoryMode): Unit = synchronized {
    releaseStorageMemory(numBytes, memoryMode)
  }

  /**
   * Execution memory currently in use, in bytes.
   */
  final def executionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed + offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   * Storage memory currently in use, in bytes.
   */
  final def storageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed + offHeapStorageMemoryPool.memoryUsed
  }

  /**
   *  On heap execution memory currently in use, in bytes.
   */
  final def onHeapExecutionMemoryUsed: Long = synchronized {
    onHeapExecutionMemoryPool.memoryUsed
  }

  /**
   *  Off heap execution memory currently in use, in bytes.
   */
  final def offHeapExecutionMemoryUsed: Long = synchronized {
    offHeapExecutionMemoryPool.memoryUsed
  }

  /**
   *  On heap storage memory currently in use, in bytes.
   */
  final def onHeapStorageMemoryUsed: Long = synchronized {
    onHeapStorageMemoryPool.memoryUsed
  }

  /**
   *  Off heap storage memory currently in use, in bytes.
   */
  final def offHeapStorageMemoryUsed: Long = synchronized {
    offHeapStorageMemoryPool.memoryUsed
  }

  /**
   * Returns the execution memory consumption, in bytes, for the given task.
   */
  private[memory] def getExecutionMemoryUsageForTask(taskAttemptId: Long): Long = synchronized {
    onHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId) +
      offHeapExecutionMemoryPool.getMemoryUsageForTask(taskAttemptId)
  }

  // -- Fields related to Tungsten managed memory -------------------------------------------------

  /**
   * Tracks whether Tungsten memory will be allocated on the JVM heap or off-heap using
   * sun.misc.Unsafe.
   */
  final val tungstenMemoryMode: MemoryMode = {
    if (conf.get(MEMORY_OFFHEAP_ENABLED)) {
      require(conf.get(MEMORY_OFFHEAP_SIZE) > 0,
        "spark.memory.offHeap.size must be > 0 when spark.memory.offHeap.enabled == true")
      require(Platform.unaligned(),
        "No support for unaligned Unsafe. Set spark.memory.offHeap.enabled to false.")
      MemoryMode.OFF_HEAP
    } else {
      MemoryMode.ON_HEAP
    }
  }

  /**
   * The default page size, in bytes.
   *
   * If user didn't explicitly set "spark.buffer.pageSize", we figure out the default value
   * by looking at the number of cores available to the process, and the total amount of memory,
   * and then divide it by a factor of safety.
   */
  val pageSizeBytes: Long = {
    val minPageSize = 1L * 1024 * 1024   // 1MB
    val maxPageSize = 64L * minPageSize  // 64MB
    val cores = if (numCores > 0) numCores else Runtime.getRuntime.availableProcessors()
    // Because of rounding to next power of 2, we may have safetyFactor as 8 in worst case
    val safetyFactor = 16
    val maxTungstenMemory: Long = tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => onHeapExecutionMemoryPool.poolSize
      case MemoryMode.OFF_HEAP => offHeapExecutionMemoryPool.poolSize
    }
    val size = ByteArrayMethods.nextPowerOf2(maxTungstenMemory / cores / safetyFactor)
    val default = math.min(maxPageSize, math.max(minPageSize, size))
    conf.get(BUFFER_PAGESIZE).getOrElse(default)
  }

  /**
   * Allocates memory for use by Unsafe/Tungsten code.
   */
  private[memory] final val tungstenMemoryAllocator: MemoryAllocator = {
    tungstenMemoryMode match {
      case MemoryMode.ON_HEAP => MemoryAllocator.HEAP
      case MemoryMode.OFF_HEAP => MemoryAllocator.UNSAFE
    }
  }
}
