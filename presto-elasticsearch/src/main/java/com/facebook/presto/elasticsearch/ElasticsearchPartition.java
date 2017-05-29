
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPartition;
import com.facebook.presto.spi.TupleDomain;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchPartition
        implements ConnectorPartition
{
    private final String schemaName;
    private final String tableName;

    public ElasticsearchPartition(String schemaName, String tableName)
    {
        this.schemaName = checkNotNull(schemaName, "schema name is null");
        this.tableName = checkNotNull(tableName, "table name is null");
    }

    @Override
    public String getPartitionId()
    {
        return schemaName + ":" + tableName;
    }

    public String getSchemaName()
    {
        return schemaName;
    }

    public String getTableName()
    {
        return tableName;
    }

    @Override
    public TupleDomain<ColumnHandle> getTupleDomain()
    {
        return TupleDomain.all();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("schemaName", schemaName)
                .add("tableName", tableName)
                .toString();
    }
}
