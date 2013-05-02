package com.facebook.presto.spi;

import java.util.Map;

public interface Partition
{
    /**
     * Get the unique id if this partition within the scope of the table.
     */
    String getPartitionId();

    /**
     * Gets the values associated with each partition key for this partition.
     */
    Map<ColumnHandle, String> getKeys();
}
