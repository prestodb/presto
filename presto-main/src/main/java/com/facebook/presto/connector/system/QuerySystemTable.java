package com.facebook.presto.connector.system;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.InMemoryRecordSet;
import com.facebook.presto.metadata.InMemoryRecordSet.Builder;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.util.List;

import static com.facebook.presto.metadata.MetadataUtil.TableMetadataBuilder.tableMetadataBuilder;
import static com.facebook.presto.metadata.MetadataUtil.columnTypeGetter;
import static com.facebook.presto.spi.ColumnType.LONG;
import static com.facebook.presto.spi.ColumnType.STRING;
import static com.google.common.collect.Iterables.transform;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class QuerySystemTable
        implements SystemTable
{
    public static final SchemaTableName QUERY_TABLE_NAME = new SchemaTableName("sys", "query");

    public static final TableMetadata QUERY_TABLE = tableMetadataBuilder(QUERY_TABLE_NAME)
            .column("query_id", STRING)
            .column("state", STRING)
            .column("user", STRING)
            .column("query", STRING)
            .column("created", LONG)
            .build();

    private final QueryManager queryManager;

    @Inject
    public QuerySystemTable(QueryManager queryManager)
    {
        this.queryManager = queryManager;
    }

    @Override
    public TableMetadata getTableMetadata()
    {
        return QUERY_TABLE;
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return ImmutableList.copyOf(transform(QUERY_TABLE.getColumns(), columnTypeGetter()));
    }

    @Override
    public RecordCursor cursor()
    {
        Builder table = InMemoryRecordSet.builder(QUERY_TABLE);
        for (QueryInfo queryInfo : queryManager.getAllQueryInfo()) {
            table.addRow(
                    queryInfo.getQueryId().toString(),
                    queryInfo.getState().toString(),
                    queryInfo.getSession().getUser(),
                    queryInfo.getQuery(),
                    MILLISECONDS.toSeconds(queryInfo.getQueryStats().getCreateTime().getMillis()));
        }
        return table.build().cursor();
    }
}
