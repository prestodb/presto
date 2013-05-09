package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaTableName;

import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionOfflineException
    extends RuntimeException
{
    private final SchemaTableName tableName;
    private final String partition;

    public PartitionOfflineException(SchemaTableName tableName, String partition)
    {
        this(tableName, partition, String.format("Table '%s' partition '%s' is offline", tableName, partition));
    }

    public PartitionOfflineException(SchemaTableName tableName,
            String partition,
            String message)
    {
        super(message);
        if (tableName == null) {
            throw new NullPointerException("tableName is null");
        }
        this.tableName = checkNotNull(tableName, "tableName is null");
        this.partition = checkNotNull(partition, "partition is null");
    }

    public SchemaTableName getTableName()
    {
        return tableName;
    }

    public String getPartition()
    {
        return partition;
    }
}
