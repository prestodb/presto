package com.facebook.presto.spi;

import java.util.List;
import java.util.Map;

public interface ConnectorSplitManager
{
    String getConnectorId();

    boolean canHandle(TableHandle handle);

    List<Partition> getPartitions(TableHandle table, Map<ColumnHandle, Object> bindings);

    Iterable<Split> getPartitionSplits(TableHandle table, List<Partition> partitions);
}
