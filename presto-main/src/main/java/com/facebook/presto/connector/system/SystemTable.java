package com.facebook.presto.connector.system;

import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.TableMetadata;

public interface SystemTable
        extends RecordSet
{
    /**
     * True if table is distributed across all nodes.
     */
    boolean isDistributed();

    TableMetadata getTableMetadata();
}
