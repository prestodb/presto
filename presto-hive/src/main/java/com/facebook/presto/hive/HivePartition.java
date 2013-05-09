package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartition
        implements Partition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final String partitionId;
    private final Map<ColumnHandle, Object> keys;

    public HivePartition(SchemaTableName tableName)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionId = UNPARTITIONED_ID;
        this.keys = ImmutableMap.of();
    }

    public HivePartition(SchemaTableName tableName, String partitionId, Map<ColumnHandle, Object> keys)
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
    public Map<ColumnHandle, Object> getKeys()
    {
        return keys;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partitionId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final HivePartition other = (HivePartition) obj;
        return Objects.equals(this.partitionId, other.partitionId);
    }

    @Override
    public String toString()
    {
        return tableName + ":" + partitionId;
    }
}
