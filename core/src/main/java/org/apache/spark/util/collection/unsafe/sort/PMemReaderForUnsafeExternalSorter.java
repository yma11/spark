package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;

import java.io.Closeable;

public final class PMemReaderForUnsafeExternalSorter extends UnsafeSorterIterator implements Closeable {
    private int recordLength;
    private long keyPrefix;
    private int numRecordsRemaining;
    private int numRecords;
    private LongArray sortedArray;
    private int position = 0;
    private byte[] arr = new byte[1024 * 1024];
    private Object baseObject = arr;
    public PMemReaderForUnsafeExternalSorter(LongArray sortedArray, int numRecords) {
        this.sortedArray = sortedArray;
        this.numRecordsRemaining = this.numRecords = numRecords;
    }
    @Override
    public void loadNext() {
        assert(position < numRecords * 2)
                : "Illegal state: Pages finished read but hasNext() is true.";
        final long address = sortedArray.get(position);
        keyPrefix = sortedArray.get(position + 1);
        int uaoSize = UnsafeAlignedOffset.getUaoSize();
        recordLength = UnsafeAlignedOffset.getSize(null, address);
        if (recordLength > arr.length) {
            arr = new byte[recordLength];
            baseObject = arr;
        }
        // System.out.println(numRecordsRemaining);
        Platform.copyMemory(null, address + uaoSize , baseObject, Platform.BYTE_ARRAY_OFFSET, recordLength);
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
}
