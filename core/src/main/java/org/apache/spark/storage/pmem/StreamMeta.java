package org.apache.spark.storage.pmem;

public class StreamMeta {
    // total size of bytes in this stream
    public long totalSize;
    // number of chunks in this stream
    public int numOfChunks;
    // need compose or not
    boolean isComplete;
}
