package org.apache.spark.storage.pmem;

public class PMemBlock {
    // address in pmem of this chunk
    public long baseAddr;

    // chunk length;
    public long offset;
}
