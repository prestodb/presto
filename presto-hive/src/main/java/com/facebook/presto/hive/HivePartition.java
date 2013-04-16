package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.SchemaTableName;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkNotNull;

public class HivePartition
        implements Partition
{
    public static final String UNPARTITIONED_ID = "<UNPARTITIONED>";

    private final SchemaTableName tableName;
    private final String partitionId;
    private final Map<ColumnHandle, String> keys;

    public HivePartition(SchemaTableName tableName)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionId = UNPARTITIONED_ID;
        this.keys = ImmutableMap.of();
    }

    public HivePartition(SchemaTableName tableName, String partitionId)
    {
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partitionId = checkNotNull(partitionId, "partitionId is null");

        LinkedHashMap<String, String> keys;
        try {
            keys = Warehouse.makeSpecFromName(partitionId);
        }
        catch (MetaException e) {
            // invalid partition id
            throw Throwables.propagate(e);
        }

        ImmutableMap.Builder<ColumnHandle, String> builder = ImmutableMap.builder();
        for (Entry<String, String> entry : keys.entrySet()) {
            ColumnHandle columnHandle = new HiveColumnHandle(entry.getKey());
            builder.put(columnHandle, entry.getValue());
        }
        this.keys = builder.build();
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
