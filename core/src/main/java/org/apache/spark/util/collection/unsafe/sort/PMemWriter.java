package org.apache.spark.util.collection.unsafe.sort;

import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.memory.MemoryBlock;

import java.util.LinkedList;

public final class PMemWriter {
    private final ShuffleWriteMetrics writeMetrics;
    private final TaskMemoryManager taskMemoryManager;
    private int numRecordsWritten = 0;
    private final LinkedList<MemoryBlock> allocatedPMemPages = new LinkedList<>();

    public PMemWriter(
            ShuffleWriteMetrics writeMetrics,
            TaskMemoryManager taskMemoryManager) {
        this.writeMetrics = writeMetrics;
        this.taskMemoryManager = taskMemoryManager;
    }
    public boolean dumpPageToPMem(MemoryBlock page, int numRecordsInPage) {
        MemoryBlock pMemBlock = taskMemoryManager.allocatePMemPage(page.size() + 4);
        if (pMemBlock != null) {
            Platform.putInt(null, pMemBlock.getBaseOffset(), numRecordsInPage);
            Platform.copyMemory(page.getBaseObject(), page.getBaseOffset(), null, pMemBlock.getBaseOffset(), page.size());
            allocatedPMemPages.add(pMemBlock);
            numRecordsWritten += numRecordsInPage;
            return true;
        }
        return false;
    }
    public int getNumRecordsWritten() { return numRecordsWritten; }
    // getReader
}
