package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.TaskContext;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.io.Closeable;
import java.util.LinkedList;

public final class PMemReader extends UnsafeSorterIterator implements Closeable {
    private int recordLength;
    private long keyPrefix;
    private int numRecords;
    private int numRecordsRemaining;
    private LinkedList<MemoryBlock> pMemPages;
    private MemoryBlock pMemPage = null;
    private int readingPageIndex = 0;
    private int readedRecordsInCurrentPage = 0;
    private int numRecordsInpage = 0;
    private long offset = 0;
    private byte[] arr = new byte[1024 * 1024];
    private Object baseObject = arr;
    private final TaskContext taskContext = TaskContext.get();
    public PMemReader(LinkedList<MemoryBlock> pMemPages, int numRecords) {
        this.pMemPages = pMemPages;
        this.numRecordsRemaining = numRecords;
    }
    @Override
    public void loadNext() {
        if(pMemPage == null || readedRecordsInCurrentPage == numRecordsInpage) {
            pMemPage = pMemPages.get(readingPageIndex++);
            readedRecordsInCurrentPage = 0;
            numRecordsInpage = Platform.getInt(null, pMemPage.getBaseOffset());
            offset = pMemPage.getBaseOffset() + 4;
        }
        keyPrefix = Platform.getLong(null, offset);
        offset += 8;
        recordLength = Platform.getInt(null, offset);
        offset += 4;
        if (recordLength > arr.length) {
            arr = new byte[recordLength];
            baseObject = arr;
        }
        Platform.copyMemory(null, offset , baseObject, Platform.BYTE_ARRAY_OFFSET, recordLength);
        offset += recordLength;
        numRecordsRemaining --;
        // read records from each page
        // record: BaseOffSet, record length, KeyPrefix, record value
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
