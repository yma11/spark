package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.util.HashMap;
import java.util.LinkedList;

public final class PMemWriter {
    private final ShuffleWriteMetrics writeMetrics;
    private final TaskMemoryManager taskMemoryManager;
    private final LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<>();
    private LongArray sortedArray;
    private HashMap<MemoryBlock, MemoryBlock> pageMap = new HashMap<>();
    private int numRecordsWritten;
    private TaskMetrics taskMetrics;
    private int position;

    public PMemWriter(
            ShuffleWriteMetrics writeMetrics, TaskMetrics taskMetrics,
            TaskMemoryManager taskMemoryManager, int numRecords) {
        this.writeMetrics = writeMetrics;
        this.taskMetrics = taskMetrics;
        this.taskMemoryManager = taskMemoryManager;
        this.numRecordsWritten = numRecords;
    }

    public boolean dumpPageToPMem(MemoryBlock page) {
        MemoryBlock pMemBlock = taskMemoryManager.allocatePMemPage(page.size());
        if (pMemBlock != null) {
            Platform.copyMemory(page.getBaseObject(), page.getBaseOffset(), null, pMemBlock.getBaseOffset(), page.size());
            writeMetrics.incBytesWritten(page.size());
            allocatedPMemPages.add(pMemBlock);
            pageMap.put(page, pMemBlock);
            return true;
        }
        return false;
    }

    public int getNumRecordsWritten() {
        return numRecordsWritten;
    }

    public LinkedList<MemoryBlock> getAllocatedPMemPages() { return allocatedPMemPages; }

    public PMemReaderForUnsafeExternalSorter getPMemReaderForUnsafeExternalSorter() {
        return new PMemReaderForUnsafeExternalSorter(sortedArray, position, numRecordsWritten, taskMetrics);
    }

    public void updateLongArray(LongArray sortedArray, int numRecords, int position) {
        this.position = position;
        while (position < numRecords * 2){
            // update recordPointer in this array
            long originalRecordPointer = sortedArray.get(position);
            MemoryBlock page = taskMemoryManager.getOriginalPage(originalRecordPointer);
            long offset = taskMemoryManager.getOffsetInPage(originalRecordPointer) - page.getBaseOffset();
            MemoryBlock pMemBlock = pageMap.get(page);
            long pMemOffset = pMemBlock.getBaseOffset() + offset;
            sortedArray.set(position, pMemOffset);
            position += 2;
        }
        this.sortedArray = sortedArray;
    }

    public LongArray getSortedArray() {
        return sortedArray;
    }

    public int getNumOfSpilledRecords() { return numRecordsWritten - position/2; }
}
