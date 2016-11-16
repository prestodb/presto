package com.facebook.presto.hdfs;

import com.facebook.presto.spi.*;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;

/**
 * Functional class to return all handle classes.
 *
 * @author jelly.guodong.jin@gmail.com
 */
public class HDFSHandleResolver
implements ConnectorHandleResolver {

    @Override
    public Class<? extends ConnectorTableHandle> getTableHandleClass() {
        return HDFSTableHandle.class;
    }

    @Override
    public Class<? extends ConnectorTableLayoutHandle> getTableLayoutHandleClass() {
        return HDFSTableLayoutHandle.class;
    }

    @Override
    public Class<? extends ColumnHandle> getColumnHandleClass() {
        return HDFSColumnHandle.class;
    }

    @Override
    public Class<? extends ConnectorSplit> getSplitClass() {
        return HDFSSplit.class;
    }

    @Override
    public Class<? extends ConnectorPartitioningHandle> getPartitioningHandleClass() {
        return HDFSPartitioningHandle.class;
    }
}
