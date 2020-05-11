package org.apache.spark.storage.pmem;

import java.util.Iterator;

/**
 * store memta info such as map between chunkID and physical baseAddr in pmem.
 * provide methods to get chunks iterator with logicalID provided.
 */
public interface PMemMetaStore {
    /**
     *
     * @param id logical ID
     * @return whether chunks exist for this logicalID
     */
    public boolean contains(byte[] id);

    /**
     *
     * @param id logical ID
     * @return
     */
    public Iterator<PMemBlock> getInputChunkIterator(byte[] id);

    /**
     * provide trunk for output stream write, need update metadata for
     * this stream, like chunkID++, totalsize, etc. need implement methods next()
     * @param id
     * @param chunkSize
     * @return
     */
    public Iterator<PMemBlock> getOutputChunkIterator(byte[] id, long chunkSize);

    /**
     * get metadata for this logical stream with format <Long + Int + boolean>
     * @param id logical ID
     * @return StreamMeta
     */
    public StreamMeta getStreamMeta(byte[] id);

    /**
     * put metadata info in pmem? or HashMap?
     * @param id logical ID
     * @param streamMeta
     */
    public void putStreamMeta(byte[] id, StreamMeta streamMeta);

}


