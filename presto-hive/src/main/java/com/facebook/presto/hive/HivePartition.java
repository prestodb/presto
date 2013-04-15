package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartition
        implements Partition
{
    private final SchemaTableName tableName;
    private final String partitionId;
    private final Map<ColumnHandle, String> keys;

    public HivePartition(SchemaTableName tableName, String partitionId, Map<ColumnHandle, String> keys)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionId = checkNotNull(partitionId, "partitionId is null");
        this.keys = ImmutableMap.copyOf(checkNotNull(keys, "keys is null"));
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    @Override
    public String getPartitionId()
    {
        return partitionId;
    }

    @Override
    public Map<ColumnHandle, String> getKeys()
    {
        return keys;
    }
}
