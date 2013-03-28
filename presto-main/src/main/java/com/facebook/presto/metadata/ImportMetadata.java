package com.facebook.presto.metadata;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.facebook.presto.ingest.ImportSchemaUtil.convertToMetadata;
import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.MetadataUtil.getTableColumns;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ImportMetadata
        extends AbstractMetadata
{
    private final ImportClientManager importClientManager;

    @Inject
    public ImportMetadata(ImportClientManager importClientManager)
    {
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
    }

    @Override
    public TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);
        ImportClient client = importClientManager.getClient(table.getCatalogName());

        List<SchemaField> tableSchema = getTableSchema(client, table.getSchemaName(), table.getTableName());

        ImportTableHandle importTableHandle = ImportTableHandle.forQualifiedTableName(table);

        List<ColumnMetadata> columns = convertToMetadata(table.getCatalogName(), tableSchema);

        return new TableMetadata(table, columns, importTableHandle);
    }

    @Override
    public QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(DataSourceType.IMPORT == tableHandle.getDataSourceType(), "not a import handle: %s", tableHandle);

        ImportTableHandle importTableHandle = (ImportTableHandle) tableHandle;

        return new QualifiedTableName(importTableHandle.getSourceName(),
                importTableHandle.getDatabaseName(),
                importTableHandle.getTableName());
    }

    @Override
    public TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkState(DataSourceType.IMPORT == tableHandle.getDataSourceType(), "not a import handle: %s", tableHandle);
        checkState(DataSourceType.IMPORT == columnHandle.getDataSourceType(), "not a import handle: %s", columnHandle);

        ImportTableHandle importTableHandle = (ImportTableHandle) tableHandle;
        ImportColumnHandle importColumnHandle = (ImportColumnHandle) columnHandle;

        return new TableColumn(importTableHandle.getTable(),
                importColumnHandle.getColumnName(),
                importColumnHandle.getColumnId(),
                importColumnHandle.getColumnType());
    }

    public boolean hasCatalog(String catalogName)
    {
        return importClientManager.hasCatalog(catalogName);
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImportClient client = importClientManager.getClient(prefix.getCatalogName());

        ImmutableList.Builder<QualifiedTableName> list = ImmutableList.builder();

        Iterable<String> schemaNames = prefix.hasSchemaName() ? prefix.getSchemaName().asSet() : getDatabaseNames(client);

        for (String schema : schemaNames) {
            List<String> tables = getTableNames(client, schema);
            for (String table : tables) {
                list.add(new QualifiedTableName(prefix.getCatalogName(), schema, table));
            }
        }
        return list.build();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        ImportClient client = importClientManager.getClient(catalogName);
        return getDatabaseNames(client);
    }

    @Override
    public List<TableColumn> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImportClient client = importClientManager.getClient(prefix.getCatalogName());

        Iterable<String> databaseNames = prefix.hasSchemaName() ? prefix.getSchemaName().asSet() : getDatabaseNames(client);

        ImmutableList.Builder<TableColumn> list = ImmutableList.builder();

        for (String databaseName : databaseNames) {
            Iterable<String> tableNames = prefix.hasTableName() ? prefix.getTableName().asSet() : getTableNames(client, databaseName);
            for (String tblName : tableNames) {
                List<SchemaField> tableSchema = getTableSchema(client, databaseName, tblName);
                Map<String, List<ColumnMetadata>> map = ImmutableMap.of(tblName, convertToMetadata(prefix.getCatalogName(), tableSchema));
                list.addAll(getTableColumns(prefix.getCatalogName(), databaseName, map));
            }
        }

        return list.build();
    }

    @Override
    public List<String> listTablePartitionKeys(QualifiedTableName table)
    {
        checkTable(table);
        ImportClient client = importClientManager.getClient(table.getCatalogName());

        ImmutableList.Builder<String> list = ImmutableList.builder();
        for (SchemaField partition : getPartitionKeys(client, table.getSchemaName(), table.getTableName())) {
            list.add(partition.getFieldName());
        }

        return list.build();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImportClient client = importClientManager.getClient(prefix.getCatalogName());

        Iterable<String> databaseNames = prefix.hasSchemaName() ? prefix.getSchemaName().asSet() : getDatabaseNames(client);

        ImmutableList.Builder<Map<String, String>> list = ImmutableList.builder();

        for (String databaseName : databaseNames) {
            Iterable<String> tableNames = prefix.hasTableName() ? prefix.getTableName().asSet() : getTableNames(client, databaseName);
            for (String tblName : tableNames) {
                for (PartitionInfo partition : getPartitions(client, databaseName, tblName)) {
                    list.add(partition.getKeyFields());
                }
            }
        }
        return list.build();
    }

    private static List<SchemaField> getTableSchema(final ImportClient client, final String database, final String table)
    {
        return retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<SchemaField>>()
                {
                    @Override
                    public List<SchemaField> call()
                            throws Exception
                    {
                        return client.getTableSchema(database, table);
                    }
                });
    }

    private static List<String> getTableNames(final ImportClient client, final String database)
    {
        return retry().stopOnIllegalExceptions().runUnchecked(new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                try {
                    return client.getTableNames(database);
                }
                catch (ObjectNotFoundException e) {
                    return Collections.emptyList();
                }
            }
        });
    }

    private static List<String> getDatabaseNames(final ImportClient client)
    {
        return retry().stopOnIllegalExceptions().runUnchecked(new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return client.getDatabaseNames();
            }
        });
    }

    private static List<SchemaField> getPartitionKeys(final ImportClient client, final String database, final String table)
    {
        return retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<SchemaField>>()
                {
                    @Override
                    public List<SchemaField> call()
                            throws Exception
                    {
                        return client.getPartitionKeys(database, table);
                    }
                });
    }

    private static List<PartitionInfo> getPartitions(final ImportClient client, final String database, final String table)
    {
        return retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<PartitionInfo>>()
                {
                    @Override
                    public List<PartitionInfo> call()
                            throws Exception
                    {
                        return client.getPartitions(database, table);
                    }
                });
    }
}
