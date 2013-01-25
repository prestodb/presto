/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.ingest.ImportSchemaUtil.convertToMetadata;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;

public class ImportTableExecution
        implements QueryExecution
{
    private static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);
    private static final List<String> FIELD_NAMES = ImmutableList.of("dummy");

    private final String queryId;
    private final Session session;
    private final URI self;
    private final ImportClientFactory importClientFactory;
    private final ImportManager importManager;
    private final Metadata metadata;
    private final String sourceName;
    private final String databaseName;
    private final String tableName;
    private final String query;
    private final QueryStats queryStats = new QueryStats();

    ImportTableExecution(
            String queryId,
            Session session,
            URI self,
            ImportClientFactory importClientFactory,
            ImportManager importManager,
            Metadata metadata,
            String sourceName,
            String databaseName,
            String tableName,
            String query)
    {
        this.queryId = queryId;
        this.session = session;
        this.self = self;
        this.importClientFactory = importClientFactory;
        this.importManager = importManager;
        this.metadata = metadata;
        this.sourceName = sourceName;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.query = query;
    }

    @Override
    public String getQueryId()
    {
        return queryId;
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return new QueryInfo(queryId,
                session,
                QueryState.FINISHED,
                self,
                FIELD_NAMES,
                query,
                queryStats,
                new StageInfo(queryId,
                        queryId + "-0",
                        StageState.FINISHED,
                        URI.create("fake://fake"),
                        null,
                        TUPLE_INFOS,
                        ImmutableList.<TaskInfo>of(),
                        ImmutableList.<StageInfo>of(),
                        ImmutableList.<FailureInfo>of()),
                ImmutableList.<FailureInfo>of());
    }

    @Override
    public void start()
    {
        queryStats.recordExecutionStart();

        String catalogName = session.getCatalog();
        String schemaName = session.getSchema();

        List<SchemaField> schema = retry().stopOn(ObjectNotFoundException.class).runUnchecked(new Callable<List<SchemaField>>()
        {
            @Override
            public List<SchemaField> call()
                    throws Exception
            {
                ImportClient importClient = importClientFactory.getClient(sourceName);
                return importClient.getTableSchema(databaseName, tableName);
            }
        });

        List<ColumnMetadata> sourceColumns = convertToMetadata(sourceName, schema);
        TableMetadata table = new TableMetadata(catalogName, schemaName, tableName, sourceColumns);
        metadata.createTable(table);

        table = metadata.getTable(catalogName, schemaName, tableName);
        long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
        List<ImportField> fields = getImportFields(sourceColumns, table.getColumns());

        importManager.importTable(tableId, sourceName, databaseName, tableName, fields);
    }

    @Override
    public void updateState()
    {
    }

    @Override
    public void cancel()
    {
        // imports are global background tasks, so canceling this "scheduling" query doesn't mean anything
    }

    private static List<ImportField> getImportFields(List<ColumnMetadata> sourceColumns, List<ColumnMetadata> targetColumns)
    {
        checkArgument(sourceColumns.size() == targetColumns.size(), "column size mismatch");
        ImmutableList.Builder<ImportField> fields = ImmutableList.builder();
        for (int i = 0; i < sourceColumns.size(); i++) {
            ImportColumnHandle sourceColumn = (ImportColumnHandle) sourceColumns.get(i).getColumnHandle().get();
            NativeColumnHandle targetColumn = (NativeColumnHandle) targetColumns.get(i).getColumnHandle().get();
            fields.add(new ImportField(sourceColumn, targetColumn));
        }
        return fields.build();
    }
}
