package com.facebook.presto.spi;

public interface SystemTable
        extends RecordSet
{
    /**
     * True if table is distributed across all nodes.
     */
    boolean isDistributed();

    TableMetadata getTableMetadata();
}
