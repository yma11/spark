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

package org.apache.spark.util.collection.unsafe.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.function.Supplier;

import com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkEnv;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.SerializerManager;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.util.Utils;

/**
 * External sorter based on {@link UnsafeInMemorySorter}.
 */
public final class UnsafeExternalSorter extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(UnsafeExternalSorter.class);

  @Nullable
  private final PrefixComparator prefixComparator;

  /**
   * {@link RecordComparator} may probably keep the reference to the records they compared last
   * time, so we should not keep a {@link RecordComparator} instance inside
   * {@link UnsafeExternalSorter}, because {@link UnsafeExternalSorter} is referenced by
   * {@link TaskContext} and thus can not be garbage collected until the end of the task.
   */
  @Nullable
  private final Supplier<RecordComparator> recordComparatorSupplier;

  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final SerializerManager serializerManager;
  private final TaskContext taskContext;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  private final int fileBufferSizeBytes;

  /**
   * Force this sorter to spill when there are this many elements in memory.
   */
  private final int numElementsForSpillThreshold;
//  private final boolean spillToPMemEnabled = true;
  private final boolean spillToPMemEnabled = SparkEnv.get() != null && (boolean) SparkEnv.get().conf().get(
         package$.MODULE$.MEMORY_SPILL_PMEM_ENABLED());
  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<UnsafeSorterSpillWriter> spillWriters = new LinkedList<>();
  private final LinkedList<PMemWriter> pMemSpillWriters = new LinkedList<>();


  // These variables are reset after spilling:
  @Nullable private volatile UnsafeInMemorySorter inMemSorter;

  private MemoryBlock currentPage = null;
  private long pageCursor = -1;
  private long peakMemoryUsedBytes = 0;
  private long totalSpillBytes = 0L;
  private long totalSortTimeNanos = 0L;
  private volatile SpillableIterator readingIterator = null;

  public static UnsafeExternalSorter createWithExistingInMemorySorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      UnsafeInMemorySorter inMemorySorter) throws IOException {
    UnsafeExternalSorter sorter = new UnsafeExternalSorter(taskMemoryManager, blockManager,
      serializerManager, taskContext, recordComparatorSupplier, prefixComparator, initialSize,
        pageSizeBytes, numElementsForSpillThreshold, inMemorySorter, false /* ignored */);
    sorter.spill(Long.MAX_VALUE, sorter);
    // The external sorter will be used to insert records, in-memory sorter is not needed.
    sorter.inMemSorter = null;
    return sorter;
  }

  public static UnsafeExternalSorter create(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      boolean canUseRadixSort) {
    return new UnsafeExternalSorter(taskMemoryManager, blockManager, serializerManager,
      taskContext, recordComparatorSupplier, prefixComparator, initialSize, pageSizeBytes,
      numElementsForSpillThreshold, null, canUseRadixSort);
  }

  private UnsafeExternalSorter(
      TaskMemoryManager taskMemoryManager,
      BlockManager blockManager,
      SerializerManager serializerManager,
      TaskContext taskContext,
      Supplier<RecordComparator> recordComparatorSupplier,
      PrefixComparator prefixComparator,
      int initialSize,
      long pageSizeBytes,
      int numElementsForSpillThreshold,
      @Nullable UnsafeInMemorySorter existingInMemorySorter,
      boolean canUseRadixSort) {
    super(taskMemoryManager, pageSizeBytes, taskMemoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = taskMemoryManager;
    this.blockManager = blockManager;
    this.serializerManager = serializerManager;
    this.taskContext = taskContext;
    this.recordComparatorSupplier = recordComparatorSupplier;
    this.prefixComparator = prefixComparator;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility for units
    // this.fileBufferSizeBytes = (int) conf.getSizeAsKb("spark.shuffle.file.buffer", "32k") * 1024
    this.fileBufferSizeBytes = 32 * 1024;

    if (existingInMemorySorter == null) {
      RecordComparator comparator = null;
      if (recordComparatorSupplier != null) {
        comparator = recordComparatorSupplier.get();
      }
      this.inMemSorter = new UnsafeInMemorySorter(
        this,
        taskMemoryManager,
        comparator,
        prefixComparator,
        initialSize,
        canUseRadixSort);
    } else {
      this.inMemSorter = existingInMemorySorter;
    }
    this.peakMemoryUsedBytes = getMemoryUsage();
    this.numElementsForSpillThreshold = numElementsForSpillThreshold;

    // Register a cleanup task with TaskContext to ensure that memory is guaranteed to be freed at
    // the end of the task. This is necessary to avoid memory leaks in when the downstream operator
    // does not fully consume the sorter's output (e.g. sort followed by limit).
    taskContext.addTaskCompletionListener(context -> {
      cleanupResources();
    });
  }

  /**
   * Marks the current page as no-more-space-available, and as a result, either allocate a
   * new page or spill when we see the next record.
   */
  @VisibleForTesting
  public void closeCurrentPage() {
    if (currentPage != null) {
      pageCursor = currentPage.getBaseOffset() + currentPage.size();
    }
  }

  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    if (trigger != this) {
      if (readingIterator != null) {
        return readingIterator.spill();
      }
      return 0L; // this should throw exception
    }

    if (inMemSorter == null || inMemSorter.numRecords() <= 0) {
      return 0L;
    }

    logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
      Thread.currentThread().getId(),
      Utils.bytesToString(getMemoryUsage()),
      spillWriters.size() + pMemSpillWriters.size(),
      (spillWriters.size() + pMemSpillWriters.size()) > 1 ? " times" : " time");
    long spillSize = 0;
    ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
    // firstly try to spill to PMem if spark.memory.spill.pmem.enabled set to true
    long arraySize = inMemSorter.getArray().memoryBlock().size();
    long required = getMemoryUsage();
    // assure there is enough PMem space for this spill first
    long sortDuration = 0;
    long writeDuration = 0;
    if (spillToPMemEnabled && taskMemoryManager.acquireExtendedMemory(required) == required) {
      // records are not sorted before spill to PMem, may affected performance?
      final PMemWriter pMemSpillWriter = new PMemWriter(writeMetrics, taskContext.taskMetrics(),
              taskMemoryManager, inMemSorter.numRecords());
      long sortTime = System.nanoTime();
      UnsafeSorterIterator sortedIterator = inMemSorter.getSortedIterator();

      sortDuration = System.nanoTime() - sortTime;
      System.out.println("inMemSorter records: " + sortedIterator.getNumRecords());
      long dumpTime = System.nanoTime();
      for (MemoryBlock page : allocatedPages) {
        if (!pMemSpillWriter.dumpPageToPMem(page)) {
          logger.error("UnsafeExternalSorter fails to spill fully to PMem.");
        }
      }
      writeDuration = System.nanoTime() - dumpTime;

      pMemSpillWriter.updateLongArray(inMemSorter.getArray(), inMemSorter.numRecords(), 0);
      // verify all records in inMemSoter are spilled in PMem
      assert(pMemSpillWriter.getNumRecordsWritten() == inMemSorter.numRecords());
      pMemSpillWriters.add(pMemSpillWriter);
      // spill size includes long array size
      spillSize += getMemoryUsage();
      freeMemory();

    } else {
    // fallback to disk spill if PMem spill is not enabled or space not enough
      final UnsafeSorterSpillWriter spillWriter =
              new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics,
                      inMemSorter.numRecords());
      spillWriters.add(spillWriter);
      long sortStartTime = System.nanoTime();
      UnsafeSorterIterator sortedIterator = inMemSorter.getSortedIterator();
      sortDuration = System.nanoTime() - sortStartTime;
      System.out.println("inMemSorter records: " + sortedIterator.getNumRecords());
      long writeStartTime = System.nanoTime();
      spillIterator(sortedIterator, spillWriter);
      writeDuration = System.nanoTime() - writeStartTime;
      spillSize += freeMemory();
    }
    inMemSorter.reset();
    // Note that this is more-or-less going to be a multiple of the page size, so wasted space in
    // pages will currently be counted as memory spilled even though that space isn't actually
    // written to disk. This also counts the space needed to store the sorter's pointer array.
    // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // pages, we might not be able to get memory for the pointer array.
    System.out.println("long array size: " + Utils.bytesToString(arraySize)
            + " released size: " + Utils.bytesToString(spillSize) + " write size: "
            + Utils.bytesToString(writeMetrics.bytesWritten()) +
            " write time: " + writeDuration/1000000);
    taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    taskContext.taskMetrics().incDiskBytesSpilled(writeMetrics.bytesWritten());
    taskContext.taskMetrics().incShuffleSpillWriteTime(writeDuration);
    taskContext.taskMetrics().incSpillSortTime(sortDuration);
    totalSpillBytes += spillSize;
    return spillSize;
  }

  /**
   * Return the total memory usage of this sorter, including the data pages and the sorter's pointer
   * array.
   */
  private long getMemoryUsage() {
    long totalPageSize = 0;
    for (MemoryBlock page : allocatedPages) {
      totalPageSize += page.size();
    }
    return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  }

  private void updatePeakMemoryUsed() {
    long mem = getMemoryUsage();
    if (mem > peakMemoryUsedBytes) {
      peakMemoryUsedBytes = mem;
    }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  public long getPeakMemoryUsedBytes() {
    updatePeakMemoryUsed();
    return peakMemoryUsedBytes;
  }

  /**
   * @return the total amount of time spent sorting data (in-memory only).
   */
  public long getSortTimeNanos() {
    UnsafeInMemorySorter sorter = inMemSorter;
    if (sorter != null) {
      return sorter.getSortTimeNanos();
    }
    return totalSortTimeNanos;
  }

  /**
   * Return the total number of bytes that has been spilled into disk so far.
   */
  public long getSpillSize() {
    return totalSpillBytes;
  }

  @VisibleForTesting
  public int getNumberOfAllocatedPages() {
    return allocatedPages.size();
  }

  /**
   * Free this sorter's data pages.
   *
   * @return the number of bytes freed.
   */
  private long freeMemory() {
    updatePeakMemoryUsed();
    long memoryFreed = 0;
    for (MemoryBlock block : allocatedPages) {
      memoryFreed += block.size();
      freePage(block);
    }
    allocatedPages.clear();
    currentPage = null;
    pageCursor = 0;
    return memoryFreed;
  }

  /**
   * Deletes any spill files created by this sorter.
   */
  private void deleteSpillFiles() {
    for (UnsafeSorterSpillWriter spill : spillWriters) {
      File file = spill.getFile();
      if (file != null && file.exists()) {
        if (!file.delete()) {
          logger.error("Was unable to delete spill file {}", file.getAbsolutePath());
        }
      }
    }
  }

  private void deletePMemSpillPages() {
    for (PMemWriter pMemWriter: pMemSpillWriters) {
/*      if (pMemWriter.getSortedArray().memoryBlock().pageNumber != MemoryBlock.FREED_IN_ALLOCATOR_PAGE_NUMBER) {
        freePMemPage(pMemWriter.getSortedArray().memoryBlock());
      }*/
      for (MemoryBlock block : pMemWriter.getAllocatedPMemPages()) {
        freePMemPage(block);
      }
    }
    pMemSpillWriters.clear();
  }

  /**
   * Frees this sorter's in-memory data structures and cleans up its spill files.
   */
  public void cleanupResources() {
    synchronized (this) {
      long startTime = System.nanoTime();
      deleteSpillFiles();
      deletePMemSpillPages();
      long duration = System.nanoTime() - startTime;
      taskContext.taskMetrics().incShuffleSpillDeleteTime(duration);
      freeMemory();
      if (inMemSorter != null) {
        inMemSorter.free();
        inMemSorter = null;
      }
    }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  private void growPointerArrayIfNecessary() throws IOException {
    assert(inMemSorter != null);
    if (!inMemSorter.hasSpaceForAnotherRecord()) {
      long used = inMemSorter.getMemoryUsage();
      LongArray array;
      try {
        // could trigger spilling
        array = allocateArray(used / 8 * 2);
      } catch (TooLargePageException e) {
        // The pointer array is too big to fix in a single page, spill.
        spill();
        return;
      } catch (SparkOutOfMemoryError e) {
        // should have trigger spilling
        if (!inMemSorter.hasSpaceForAnotherRecord()) {
          logger.error("Unable to grow the pointer array");
          throw e;
        }
        return;
      }
      // check if spilling is triggered or not
      if (inMemSorter.hasSpaceForAnotherRecord()) {
        freeArray(array);
      } else {
        inMemSorter.expandPointerArray(array);
      }
    }
  }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private void acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size()) {
      // TODO: try to find space on previous pages
      currentPage = allocatePage(required);
      pageCursor = currentPage.getBaseOffset();
      allocatedPages.add(currentPage);
    }
  }

  /**
   * Write a record to the sorter.
   */
  public void insertRecord(
      Object recordBase, long recordOffset, int length, long prefix, boolean prefixIsNull)
    throws IOException {

    assert(inMemSorter != null);
    if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
      logger.info("Spilling data because number of spilledRecords crossed the threshold " +
        numElementsForSpillThreshold);
      spill();
    }

    growPointerArrayIfNecessary();
    int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // Need 4 or 8 bytes to store the record length.
    final int required = length + uaoSize;
    acquireNewPageIfNecessary(required);

    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    inMemSorter.insertRecord(recordAddress, prefix, prefixIsNull);
  }

  /**
   * Write a key-value record to the sorter. The key and value will be put together in-memory,
   * using the following format:
   *
   * record length (4 bytes), key length (4 bytes), key data, value data
   *
   * record length = key length + value length + 4
   */
  public void insertKVRecord(Object keyBase, long keyOffset, int keyLen,
      Object valueBase, long valueOffset, int valueLen, long prefix, boolean prefixIsNull)
    throws IOException {

    growPointerArrayIfNecessary();
    int uaoSize = UnsafeAlignedOffset.getUaoSize();
    final int required = keyLen + valueLen + (2 * uaoSize);
    acquireNewPageIfNecessary(required);

    final Object base = currentPage.getBaseObject();
    final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);
    UnsafeAlignedOffset.putSize(base, pageCursor, keyLen + valueLen + uaoSize);
    pageCursor += uaoSize;
    UnsafeAlignedOffset.putSize(base, pageCursor, keyLen);
    pageCursor += uaoSize;
    Platform.copyMemory(keyBase, keyOffset, base, pageCursor, keyLen);
    pageCursor += keyLen;
    Platform.copyMemory(valueBase, valueOffset, base, pageCursor, valueLen);
    pageCursor += valueLen;
    assert(inMemSorter != null);
    inMemSorter.insertRecord(recordAddress, prefix, prefixIsNull);
  }

  /**
   * Merges another UnsafeExternalSorters into this one, the other one will be emptied.
   */
  public void merge(UnsafeExternalSorter other) throws IOException {
    other.spill();
    spillWriters.addAll(other.spillWriters);
    pMemSpillWriters.addAll(other.pMemSpillWriters);
    // remove them from `spillWriters`, or the files will be deleted in `cleanupResources`.
    other.spillWriters.clear();
    other.pMemSpillWriters.clear();
    other.cleanupResources();
  }

  /**
   * Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   */
  public UnsafeSorterIterator getSortedIterator() throws IOException {
    assert(recordComparatorSupplier != null);
    if (spillWriters.isEmpty() && pMemSpillWriters.isEmpty()) {
      assert(inMemSorter != null);
      readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
      return readingIterator;
    } else {
      final UnsafeSorterSpillMerger spillMerger = new UnsafeSorterSpillMerger(
        recordComparatorSupplier.get(), prefixComparator, spillWriters.size() + pMemSpillWriters.size());
      if (!spillWriters.isEmpty()) {
        for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
          spillMerger.addSpillIfNotEmpty(spillWriter.getReader(serializerManager,
                  taskContext.taskMetrics()));
        }
      }
      if (!pMemSpillWriters.isEmpty()) {
        for (PMemWriter pMemWriter: pMemSpillWriters) {
          spillMerger.addSpillIfNotEmpty(pMemWriter.getPMemReaderForUnsafeExternalSorter());
        }
      }
      if (inMemSorter != null) {
        readingIterator = new SpillableIterator(inMemSorter.getSortedIterator());
        spillMerger.addSpillIfNotEmpty(readingIterator);
      }
      return spillMerger.getSortedIterator();
    }
  }

  @VisibleForTesting boolean hasSpaceForAnotherRecord() {
    return inMemSorter.hasSpaceForAnotherRecord();
  }

  private static void spillIterator(UnsafeSorterIterator inMemIterator,
      UnsafeSorterSpillWriter spillWriter) throws IOException {
    while (inMemIterator.hasNext()) {
      inMemIterator.loadNext();
      final Object baseObject = inMemIterator.getBaseObject();
      final long baseOffset = inMemIterator.getBaseOffset();
      final int recordLength = inMemIterator.getRecordLength();
      spillWriter.write(baseObject, baseOffset, recordLength, inMemIterator.getKeyPrefix());
    }
    spillWriter.close();
  }

  /**
   * An UnsafeSorterIterator that support spilling.
   */
  class SpillableIterator extends UnsafeSorterIterator {
    private UnsafeSorterIterator upstream;
    private UnsafeSorterIterator nextUpstream = null;
    private MemoryBlock lastPage = null;
    private boolean loaded = false;
    private int numRecords = 0;

    SpillableIterator(UnsafeSorterIterator inMemIterator) {
      this.upstream = inMemIterator;
      this.numRecords = inMemIterator.getNumRecords();
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    public long spill() throws IOException {
      synchronized (this) {
        if (!(upstream instanceof UnsafeInMemorySorter.SortedIterator && nextUpstream == null
          && numRecords > 0)) {
          return 0L;
        }

        UnsafeInMemorySorter.SortedIterator inMemIterator =
          ((UnsafeInMemorySorter.SortedIterator) upstream).clone();

        ShuffleWriteMetrics writeMetrics = new ShuffleWriteMetrics();
        long arraySize = inMemSorter.getArray().memoryBlock().size();
        long required = getMemoryUsage();
        long startTime = System.nanoTime();
        long released = 0L;
        // assure there is enough PMem space for this spill first
        if (spillToPMemEnabled && taskMemoryManager.acquireExtendedMemory(required) == required) {
          final PMemWriter pMemSpillWriter = new PMemWriter(writeMetrics,
                  taskContext.taskMetrics(), taskMemoryManager, inMemIterator.getNumRecords());
          System.out.println("inMemIterator/position:" + inMemIterator.getNumRecords()
                  + " " + inMemIterator.getPosition());
          for (MemoryBlock page : allocatedPages) {
            if (!pMemSpillWriter.dumpPageToPMem(page)) {
              logger.error("UnsafeExternalSorter fails to spill fully to PMem.");
            }
          }
          pMemSpillWriter.updateLongArray(inMemSorter.getArray(), inMemIterator.getNumRecords(), inMemIterator.getPosition());
          assert(pMemSpillWriter.getNumRecordsWritten() == inMemIterator.getNumRecords());
          // pages will be freed later
          pMemSpillWriters.add(pMemSpillWriter);
          nextUpstream = pMemSpillWriter.getPMemReaderForUnsafeExternalSorter();
          // in-memory sorter will not be used after spilling
          assert(inMemSorter != null);
          released += inMemSorter.getMemoryUsage();
          totalSortTimeNanos += inMemSorter.getSortTimeNanos();
          inMemSorter.free();
        } else {
          // Iterate over the records that have not been returned and spill them.
          final UnsafeSorterSpillWriter spillWriter =
                  new UnsafeSorterSpillWriter(blockManager, fileBufferSizeBytes, writeMetrics, numRecords);
          spillIterator(inMemIterator, spillWriter);
          spillWriters.add(spillWriter);
          nextUpstream = spillWriter.getReader(serializerManager, taskContext.taskMetrics());
          // in-memory sorter will not be used after spilling
          assert(inMemSorter != null);
          released += inMemSorter.getMemoryUsage();
          totalSortTimeNanos += inMemSorter.getSortTimeNanos();
          inMemSorter.free();
        }
        long duration = System.nanoTime() - startTime;
        synchronized (UnsafeExternalSorter.this) {
          // release the pages except the one that is used. There can still be a caller that
          // is accessing the current record. We free this page in that caller's next loadNext()
          // call.
          for (MemoryBlock page : allocatedPages) {
            if (!loaded || page.pageNumber !=
                    ((UnsafeInMemorySorter.SortedIterator)upstream).getCurrentPageNumber()) {
              released += page.size();
              freePage(page);
            } else {
              lastPage = page;
            }
          }
          allocatedPages.clear();
        }
        System.out.println("long array size: " + Utils.bytesToString(arraySize)
        + "released size: " + Utils.bytesToString(released) + "write size: "
                + Utils.bytesToString(writeMetrics.bytesWritten()) +
                "write time: " + duration/1000000);
        inMemSorter = null;
        taskContext.taskMetrics().incMemoryBytesSpilled(released);
        taskContext.taskMetrics().incDiskBytesSpilled(writeMetrics.bytesWritten());
        taskContext.taskMetrics().incShuffleSpillWriteTime(duration);
        totalSpillBytes += released;
        return released;
      }
    }

    @Override
    public boolean hasNext() {
      return numRecords > 0;
    }

    @Override
    public void loadNext() throws IOException {
      MemoryBlock pageToFree = null;
      try {
        synchronized (this) {
          loaded = true;
          if (nextUpstream != null) {
            // Just consumed the last record from in memory iterator
            if(lastPage != null) {
              // Do not free the page here, while we are locking `SpillableIterator`. The `freePage`
              // method locks the `TaskMemoryManager`, and it's a bad idea to lock 2 objects in
              // sequence. We may hit dead lock if another thread locks `TaskMemoryManager` and
              // `SpillableIterator` in sequence, which may happen in
              // `TaskMemoryManager.acquireExecutionMemory`.
              pageToFree = lastPage;
              lastPage = null;
            }
            upstream = nextUpstream;
            nextUpstream = null;
          }
          if (upstream instanceof PMemReaderForUnsafeExternalSorter) {
            assert(((PMemReaderForUnsafeExternalSorter)upstream).getNumRecordsRemaining() == numRecords);
          }
          numRecords--;
          upstream.loadNext();
        }
      } finally {
        if (pageToFree != null) {
          freePage(pageToFree);
        }
      }
    }

    @Override
    public Object getBaseObject() {
      return upstream.getBaseObject();
    }

    @Override
    public long getBaseOffset() {
      return upstream.getBaseOffset();
    }

    @Override
    public int getRecordLength() {
      return upstream.getRecordLength();
    }

    @Override
    public long getKeyPrefix() {
      return upstream.getKeyPrefix();
    }
  }

  /**
   * Returns an iterator starts from startIndex, which will return the rows in the order as
   * inserted.
   *
   * It is the caller's responsibility to call `cleanupResources()`
   * after consuming this iterator.
   *
   * TODO: support forced spilling
   */
  public UnsafeSorterIterator getIterator(int startIndex) throws IOException {
    if (spillWriters.isEmpty() && pMemSpillWriters.isEmpty()) {
      assert(inMemSorter != null);
      UnsafeSorterIterator iter = inMemSorter.getSortedIterator();
      moveOver(iter, startIndex);
      return iter;
    } else {
      LinkedList<UnsafeSorterIterator> queue = new LinkedList<>();
      int i = 0;
      for (UnsafeSorterSpillWriter spillWriter : spillWriters) {
        if (i + spillWriter.recordsSpilled() > startIndex) {
          UnsafeSorterIterator iter =
                  spillWriter.getReader(serializerManager, taskContext.taskMetrics());
          moveOver(iter, startIndex - i);
          queue.add(iter);
        }
        i += spillWriter.recordsSpilled();
      }
      for (PMemWriter pMemWriter : pMemSpillWriters) {
        if (i + pMemWriter.getNumOfSpilledRecords() > startIndex) {
          UnsafeSorterIterator iter = pMemWriter.getPMemReaderForUnsafeExternalSorter();
          moveOver(iter, startIndex - i);
          queue.add(iter);
        }
        i += pMemWriter.getNumRecordsWritten();
      }
      if (inMemSorter != null) {
        UnsafeSorterIterator iter = inMemSorter.getSortedIterator();
        moveOver(iter, startIndex - i);
        queue.add(iter);
      }
      return new ChainedIterator(queue);
    }
  }

  private void moveOver(UnsafeSorterIterator iter, int steps)
      throws IOException {
    if (steps > 0) {
      for (int i = 0; i < steps; i++) {
        if (iter.hasNext()) {
          iter.loadNext();
        } else {
          throw new ArrayIndexOutOfBoundsException("Failed to move the iterator " + steps +
            " steps forward");
        }
      }
    }
  }

  /**
   * Chain multiple UnsafeSorterIterator together as single one.
   */
  static class ChainedIterator extends UnsafeSorterIterator {

    private final Queue<UnsafeSorterIterator> iterators;
    private UnsafeSorterIterator current;
    private int numRecords;

    ChainedIterator(Queue<UnsafeSorterIterator> iterators) {
      assert iterators.size() > 0;
      this.numRecords = 0;
      for (UnsafeSorterIterator iter: iterators) {
        this.numRecords += iter.getNumRecords();
      }
      this.iterators = iterators;
      this.current = iterators.remove();
    }

    @Override
    public int getNumRecords() {
      return numRecords;
    }

    @Override
    public boolean hasNext() {
      while (!current.hasNext() && !iterators.isEmpty()) {
        current = iterators.remove();
      }
      return current.hasNext();
    }

    @Override
    public void loadNext() throws IOException {
      while (!current.hasNext() && !iterators.isEmpty()) {
        current = iterators.remove();
      }
      current.loadNext();
    }

    @Override
    public Object getBaseObject() { return current.getBaseObject(); }

    @Override
    public long getBaseOffset() { return current.getBaseOffset(); }

    @Override
    public int getRecordLength() { return current.getRecordLength(); }

    @Override
    public long getKeyPrefix() { return current.getKeyPrefix(); }
  }
}
