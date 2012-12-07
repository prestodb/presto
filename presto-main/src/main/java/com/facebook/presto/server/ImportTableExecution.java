/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class ImportTableExecution
        implements QueryExecution
{
    private static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);
    private static final List<String> FIELD_NAMES = ImmutableList.of("dummy");

    private final String queryId;
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final String sourceName;
    private final String databaseName;
    private final String tableName;

    ImportTableExecution(
            String queryId,
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            String sourceName,
            String databaseName,
            String tableName)
    {
        this.queryId = queryId;
        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.sourceName = sourceName;
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return new QueryInfo(queryId, TUPLE_INFOS, FIELD_NAMES, QueryState.FINISHED, null, ImmutableMap.<String, List<TaskInfo>>of());
    }

    @Override
    public void start()
    {
        String catalogName = "default";
        String schemaName = "default";

        ImportClient importClient = importClientFactory.getClient(sourceName);
        List<SchemaField> schema = importClient.getTableSchema(databaseName, tableName);
        List<ColumnMetadata> columns = ImportSchemaUtil.createColumnMetadata(schema);
        TableMetadata table = new TableMetadata(catalogName, schemaName, tableName, columns);
        metadata.createTable(table);

        table = metadata.getTable(catalogName, schemaName, tableName);
        long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
        List<ImportField> fields = getImportFields(table.getColumns());

        importManager.importTable(tableId, sourceName, databaseName, tableName, fields);
    }

    @Override
    public void cancel()
    {
        // imports are global background tasks, so canceling this "scheduling" query doesn't mean anything
    }

    private static List<ImportField> getImportFields(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<ImportField> fields = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            long columnId = ((NativeColumnHandle) column.getColumnHandle().get()).getColumnId();
            fields.add(new ImportField(columnId, column.getType(), column.getName()));
        }
        return fields.build();
    }
}
