package org.apache.spark.storage.pmem;

/**
 * expose methods for high level stream to write/read data from different backend
 */
public interface ChunkAPI {
    /**
     * trunkID: logicalID + trunkIndex
     * @param id  unique trunkID, mapping to physical pmem address
     * @param value value to write on pmem
     */
    public void write(byte[] id, byte[] value);

    /**
     * check whether this trunk exists
     * @param id trunkID
     * @return
     */
    public boolean contains(byte[] id);

    /**
     *
     * @param id trunkID
     * @return base address of trunk with id
     */
    public long getChunk(byte[] id);


    /**
     * update pMemManager.pMemMetaStore with <trunkID, pMemBlock>   ?????
     * @param id
     * @param pMemBlock
     */
    public void putChunk(byte[] id, PMemBlock pMemBlock);

    /**
     *
     * @param id unique trunkID
     * @param offset start position of read
     * @param len  length of bytes to read
     * @return
     */
    public boolean read(byte[] id, int offset, int len);

    /**
     * action when read done of current Chunk
     */
    public void release();

    /**
     * free pmem space when chunk life cycle end
     * @param pMemBlock
     */
    public void free(PMemBlock pMemBlock);

}
