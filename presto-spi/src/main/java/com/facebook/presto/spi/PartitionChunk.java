package com.facebook.presto.spi;

import java.net.InetAddress;
import java.util.List;

public interface PartitionChunk
{
    /**
     * Returns the name of the partition that this chunk belongs to.
     */
    String getPartitionName();

    /**
     * Marks the last chunk for a partition. This is transient and only valid
     * while iterating over the return value from {@link ImportClient#getPartitionChunks(String, String, List, List)} or
     * {@link ImportClient#getPartitionChunks(String, String, String, List)}.
     *
     * If a partition chunk returns true on this method, then no more chunks for the partition named by
     * {@link PartitionChunk#getPartitionName()} will be returned.
     */
    boolean isLastChunk();

    long getLength();

    List<InetAddress> getHosts();

    Object getInfo();
}
