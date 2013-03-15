/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.importer.ImportField;
import com.facebook.presto.importer.ImportManager;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.DataSourceType;
import com.facebook.presto.metadata.ImportColumnHandle;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.NativeColumnHandle;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientManager;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.CreateOrReplaceMaterializedView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.execution.QueryInfo.addFailure;
import static com.facebook.presto.execution.QueryInfo.getQueryId;
import static com.facebook.presto.execution.QueryInfo.getQueryStats;
import static com.facebook.presto.execution.QueryInfo.getSession;
import static com.facebook.presto.ingest.ImportSchemaUtil.convertToMetadata;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class CreateOrReplaceMaterializedViewExecution
    implements QueryExecution
{
    private static final Logger log = Logger.get(CreateOrReplaceMaterializedViewExecution.class);

    private final CreateOrReplaceMaterializedView statement;
    private final ImportClientManager importClientManager;
    private final ImportManager importManager;
    private final MetadataManager metadataManager;
    private final Sitevars sitevars;
    private final QueryMonitor queryMonitor;

    private final QueryExecutionState queryExecutionState = new QueryExecutionState();
    private final AtomicReference<QueryInfo> queryInfo = new AtomicReference<>();

    CreateOrReplaceMaterializedViewExecution(Statement statement,
            QueryInfo queryInfo,
            CreateOrReplaceMaterializedView statement,
            ImportClientManager importClientManager,
            ImportManager importManager,
            MetadataManager metadataManager,
            QueryMonitor queryMonitor,
            Sitevars sitevars)
    {
        this.queryMonitor = queryMonitor;
        this.queryInfo.set(queryInfo);
        this.statement = statement;
        this.importClientManager = importClientManager;
        this.importManager = importManager;
        this.metadataManager = metadataManager;
        this.sitevars = sitevars;
    }

    @Override
    public void start()
    {
        try {
            checkState(sitevars.isImportsEnabled(), "materialized view creation is disabled");

            // transition to starting
            queryExecutionState.transitionToStartingState();

            getQueryStats(queryInfo).recordExecutionStart();

            importTable();
        }
        catch (Exception e) {
            fail(e);
        }
    }

    @Override
    public void cancel()
    {
        // transition to canceled state, only if not already finished
        if (!queryExecutionState.isDone()) {
            log.debug("Cancelling query %s", getQueryId(queryInfo));
            queryExecutionState.toState(QueryState.CANCELED);
            getQueryStats(queryInfo).recordEnd();
            queryMonitor.completionEvent(getQueryInfo());
        }
    }

    @Override
    public void fail(Throwable cause)
    {
        if (!queryExecutionState.isDone()) {
            log.debug("Failing query %s", getQueryId(queryInfo));
            queryExecutionState.toState(QueryState.FAILED);
            addFailure(queryInfo, cause);
            getQueryStats(queryInfo).recordEnd();
            queryMonitor.completionEvent(getQueryInfo());
        }
    }

    @Override
    public void updateState(boolean forceUpdate)
    {
    }

    @Override
    public void cancelStage(String stageId)
    {
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        while(true) {
            QueryInfo currentQueryInfo = queryInfo.get();

            QueryInfo newQueryInfo = currentQueryInfo
                .queryState(queryExecutionState.getQueryState());

            if(queryInfo.compareAndSet(currentQueryInfo, newQueryInfo)) {
                return newQueryInfo;
            }
        }
    }

    private void importTable()
        throws Exception
    {
        QualifiedTableName dstTableName = MetadataUtil.createQualifiedTableName(getSession(queryInfo), statement.getName());

        checkState(DataSourceType.NATIVE == metadataManager.lookupDataSource(dstTableName.getCatalogName(), dstTableName.getSchemaName(), dstTableName.getTableName()), "%s is not a native table, can only create native tables", dstTableName);
        checkState(statement.getTableDefinition() instanceof Query, "Can only create a table from a query");

        Query subQuery = (Query) statement.getTableDefinition();

        List<Relation> relations = subQuery.getFrom();
        Relation srcTableRelation = Iterables.getOnlyElement(relations);
        checkState(srcTableRelation instanceof Table, "create table queries must use as table as source");

        Select select = subQuery.getSelect();
        checkState(Iterables.getOnlyElement(select.getSelectItems()) instanceof AllColumns, "create table query can have only a single column and it must be '*'");

        final QualifiedTableName srcTableName = MetadataUtil.createQualifiedTableName(getSession(queryInfo), ((Table) srcTableRelation).getName());

        checkState(DataSourceType.IMPORT == metadataManager.lookupDataSource(srcTableName.getCatalogName(), srcTableName.getSchemaName(), srcTableName.getTableName()), "Can not import from %s, not an importable table", srcTableName);

        List<SchemaField> schema = retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<SchemaField>>()
                {
                    @Override
                    public List<SchemaField> call()
                            throws Exception
                    {
                        ImportClient importClient = importClientManager.getClient(srcTableName.getCatalogName());
                        return importClient.getTableSchema(srcTableName.getSchemaName(), srcTableName.getTableName());
                    }
                });

        List<ColumnMetadata> sourceColumns = convertToMetadata(srcTableName.getCatalogName(), schema);
        TableMetadata table = new TableMetadata(dstTableName.getCatalogName(), dstTableName.getSchemaName(), dstTableName.getTableName(), sourceColumns);
        metadataManager.createTable(table);

        table = metadataManager.getTable(dstTableName.getCatalogName(), dstTableName.getSchemaName(), dstTableName.getTableName());
        long tableId = ((NativeTableHandle) table.getTableHandle().get()).getTableId();
        List<ImportField> fields = getImportFields(sourceColumns, table.getColumns());

        importManager.importTable(tableId, srcTableName.getCatalogName(), srcTableName.getSchemaName(), srcTableName.getTableName(), fields);
    }

    public static List<ImportField> getImportFields(List<ColumnMetadata> sourceColumns, List<ColumnMetadata> targetColumns)
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

    public static class CreateOrReplaceMaterializedViewExecutionFactory
            implements SimpleQueryExecutionFactory<CreateOrReplaceMaterializedViewExecution>
    {
        private final ImportClientManager importClientManager;
        private final ImportManager importManager;
        private final MetadataManager metadataManager;
        private final Sitevars sitevars;
        private final QueryMonitor queryMonitor;

        @Inject
        CreateOrReplaceMaterializedViewExecutionFactory(ImportClientManager importClientManager,
                ImportManager importManager,
                MetadataManager metadataManager,
                QueryMonitor queryMonitor,
                Sitevars sitevars)
        {

            this.importClientManager = checkNotNull(importClientManager, "importClientManager is null");
            this.importManager = checkNotNull(importManager, "importManager is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
            this.sitevars = checkNotNull(sitevars, "sitevars is null");
            this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
        }

        public CreateOrReplaceMaterializedViewExecution createQueryExecution(
                Statement statement,
                QueryInfo queryInfo)
        {
<<<<<<< HEAD
            CreateOrReplaceMaterializedViewExecution queryExecution = new CreateOrReplaceMaterializedViewExecution(statement,
                    queryInfo,
=======
            CreateOrReplaceMaterializedViewExecution queryExecution = new CreateOrReplaceMaterializedViewExecution(queryId,
                    query,
                    session,
                    locationFactory.createQueryLocation(queryId),
                    (CreateOrReplaceMaterializedView) statement,
>>>>>>> 87150bc... fixup
                    importClientManager,
                    importManager,
                    metadataManager,
                    queryMonitor,
                    sitevars);

            queryMonitor.createdEvent(queryExecution.getQueryInfo());
            return queryExecution;
        }

    }

}
