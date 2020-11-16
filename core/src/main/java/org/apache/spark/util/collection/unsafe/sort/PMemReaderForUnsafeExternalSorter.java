package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;

import java.io.Closeable;

public final class PMemReaderForUnsafeExternalSorter extends UnsafeSorterIterator implements Closeable {
    private int recordLength;
    private long keyPrefix;
    private int numRecordsRemaining;
    private int numRecords;
    private LongArray sortedArray;
    private int position;
    private byte[] arr = new byte[1024 * 1024];
    private Object baseObject = arr;
    private TaskMetrics taskMetrics;
    private long startTime;
    private long address;
    public PMemReaderForUnsafeExternalSorter(
            LongArray sortedArray, int position,  int numRecords, TaskMetrics taskMetrics) {
        this.sortedArray = sortedArray;
        this.position = position;
        this.numRecords = numRecords;
        this.numRecordsRemaining = numRecords - position/2;
        this.taskMetrics = taskMetrics
;    }
    @Override
    public void loadNext() {
        assert(position < numRecords * 2)
                : "Illegal state: Pages finished read but hasNext() is true.";
        address = sortedArray.get(position);
        keyPrefix = sortedArray.get(position + 1);
        startTime = System.nanoTime();
        recordLength = Platform.getInt(null, address);
        if (recordLength > arr.length) {
            arr = new byte[recordLength];
            baseObject = arr;
        }
        Platform.copyMemory(null, address + Integer.BYTES , baseObject, Platform.BYTE_ARRAY_OFFSET, recordLength);
        taskMetrics.incShuffleSpillReadTime(System.nanoTime() - startTime);
        numRecordsRemaining --;
        position += 2;
    }
    @Override
    public int getNumRecords() {
        return numRecords;
    }

    @Override
    public boolean hasNext() {
        return (numRecordsRemaining > 0);
    }

    @Override
    public Object getBaseObject() {
        return baseObject;
    }

    @Override
    public long getBaseOffset() {
        return Platform.BYTE_ARRAY_OFFSET;
    }

    @Override
    public int getRecordLength() {
        return recordLength;
    }

    @Override
    public long getKeyPrefix() {
        return keyPrefix;
    }

    @Override
    public void close() {
        // do nothing here
    }
    public int getNumRecordsRemaining() { return numRecordsRemaining; }
}
