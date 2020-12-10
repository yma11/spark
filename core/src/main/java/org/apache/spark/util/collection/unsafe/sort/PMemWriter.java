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

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.util.HashMap;
import java.util.LinkedList;

/**
 * Todo: the logic in this spill writer is moved from previous implementation in UnsafeExternalSorter
 * Need to check whether it's still correct later.
 */
public final class PMemWriter extends UnsafeSorterPMemSpillWriter {
    private LongArray sortedArray;
    private HashMap<MemoryBlock, MemoryBlock> pageMap = new HashMap<MemoryBlock, MemoryBlock>();
    private int numRecordsWritten;
    private int position;
    private LinkedList<MemoryBlock> allocatedDramPages;

    public PMemWriter(
            UnsafeExternalSorter externalSorter,
            SortedIteratorForSpills sortedIterator,
            int numRecordsToWritten,
            ShuffleWriteMetrics writeMetrics,
            TaskMetrics taskMetrics) {
        super(externalSorter, sortedIterator, numRecordsToWritten, writeMetrics, taskMetrics);
        this.numRecordsWritten = sortedIterator.getNumRecords();
        this.allocatedDramPages = externalSorter.getAllocatedPages();
        this.sortedArray = sortedIterator.getLongArray();
    }

    private boolean dumpPageToPMem(MemoryBlock page) {
        MemoryBlock pMemBlock = allocatePMemPage(page.size());
        if (pMemBlock != null) {
            Platform.copyMemory(page.getBaseObject(), page.getBaseOffset(), null, pMemBlock.getBaseOffset(), page.size());
            writeMetrics.incBytesWritten(page.size());
            pageMap.put(page, pMemBlock);
            return true;
        }
        return false;
    }

    public int getNumRecordsWritten() {
        return numRecordsWritten;
    }

    public PMemReaderForUnsafeExternalSorter getPMemReaderForUnsafeExternalSorter() {
        return new PMemReaderForUnsafeExternalSorter(sortedArray, position, numRecordsWritten, taskMetrics);
    }

    @Override
    public void write() {
        long dumpTime = System.nanoTime();
        for (MemoryBlock page : allocatedDramPages) {
            dumpPageToPMem(page);
        }
        long dumpDuration = System.nanoTime() - dumpTime;
        System.out.println("dump time : " + dumpDuration / 1000000);
        long sortTime = System.nanoTime();
        updateLongArray(numRecordsWritten, 0);
        long sortDuration = System.nanoTime() - sortTime;
        System.out.println("sort time : " + sortDuration / 1000000);
    }

    @Override
    public UnsafeSorterIterator getSpillReader() {
        return new PMemReaderForUnsafeExternalSorter(sortedArray, position, numRecordsWritten, taskMetrics);
    }

    public void clearAll() {
        externalSorter.freeArray(sortedArray);
        freeAllPMemPages();
    }

    private void updateLongArray(int numRecords, int position) {
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

    @Override
    public int recordsSpilled() {
        //todo: implements recordsSpill here.
        return 0;
    }
}
