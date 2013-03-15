/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.execution.QueryInfo.QueryInfoFactory;
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
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.CreateOrReplaceMaterializedView;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.ingest.ImportSchemaUtil.convertToMetadata;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class CreateOrReplaceMaterializedViewExecution
        extends SimpleSqlExecution
{
    private final CreateOrReplaceMaterializedView statement;
    private final Session session;
    private final ImportClientManager importClientManager;
    private final ImportManager importManager;
    private final MetadataManager metadataManager;

    CreateOrReplaceMaterializedViewExecution(String queryId,
            Statement statement,
            Session session,
            URI self,
            ImportClientManager importClientManager,
            ImportManager importManager,
            MetadataManager metadataManager,
            QueryMonitor queryMonitor,
            QueryInfoFactory queryInfoFactory)
    {
        super(queryId, self, queryMonitor, queryInfoFactory);

        this.statement = (CreateOrReplaceMaterializedView) statement;
        this.session = session;
        this.importClientManager = importClientManager;
        this.importManager = importManager;
        this.metadataManager = metadataManager;
    }

    @Override
    public void doStart()
            throws Exception
    {
        QualifiedTableName dstTableName = MetadataUtil.createQualifiedTableName(session, statement.getName());

        checkState(DataSourceType.NATIVE == metadataManager.lookupDataSource(dstTableName.getCatalogName(), dstTableName.getSchemaName(), dstTableName.getTableName()), "%s is not a native table, can only create native tables", dstTableName);
        checkState(statement.getTableDefinition() instanceof Query, "Can only create a table from a query");

        Query subQuery = (Query) statement.getTableDefinition();

        List<Relation> relations = subQuery.getFrom();
        Relation srcTableRelation = Iterables.getOnlyElement(relations);
        checkState(srcTableRelation instanceof Table, "create table queries must use as table as source");

        Select select = subQuery.getSelect();
        checkState(Iterables.getOnlyElement(select.getSelectItems()) instanceof AllColumns, "create table query can have only a single column and it must be '*'");

        final QualifiedTableName srcTableName = MetadataUtil.createQualifiedTableName(session, ((Table) srcTableRelation).getName());

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

    public static class CreateTableExecutionFactory
            extends SimpleSqlExecutionFactory<CreateOrReplaceMaterializedViewExecution>
    {
        private final ImportClientManager importClientManager;
        private final ImportManager importManager;
        private final MetadataManager metadataManager;

        @Inject
        CreateTableExecutionFactory(ImportClientManager importClientManager,
                ImportManager importManager,
                MetadataManager metadataManager,
                QueryMonitor queryMonitor)
        {
            super(queryMonitor);

            this.importClientManager = checkNotNull(importClientManager, "importClientManager is null");
            this.importManager = checkNotNull(importManager, "importManager is null");
            this.metadataManager = checkNotNull(metadataManager, "metadataManager is null");
        }

        public CreateOrReplaceMaterializedViewExecution createQueryExecution(String queryId,
                Statement statement,
                Session session,
                URI selfLocation,
                QueryInfoFactory queryInfoFactory)
        {
            return new CreateOrReplaceMaterializedViewExecution(queryId, statement, session, selfLocation, importClientManager, importManager, metadataManager, queryMonitor, queryInfoFactory);
        }

    }

}
