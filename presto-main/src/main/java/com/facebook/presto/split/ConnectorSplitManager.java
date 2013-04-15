package com.facebook.presto.split;

import com.facebook.presto.execution.DataSource;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.TableHandle;

import java.util.List;
import java.util.Map;

public interface ConnectorSplitManager
{
    boolean canHandle(TableHandle handle);

    List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings);

    DataSource getPartitionSplits(List<Partition> partitions, List<ColumnHandle> columnNames);
}
