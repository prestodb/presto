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
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ImportMetadata
        implements ConnectorMetadata
{
    private final ImportClientManager importClientManager;

    @Inject
    public ImportMetadata(ImportClientManager importClientManager)
    {
        this.importClientManager = checkNotNull(importClientManager, "importClientFactory is null");
    }

    @Override
    public int priority()
    {
        return 0;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof ImportTableHandle;
    }

    @Override
    public boolean canHandle(QualifiedTablePrefix prefix)
    {
        return importClientManager.hasCatalog(prefix.getCatalogName());
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        ImportClient client = importClientManager.getClient(catalogName);
        return getDatabaseNames(client);
    }

    @Override
    public TableHandle getTableHandle(QualifiedTableName tableName)
    {
        // verify table exists
        try {
            getTableMetadata(tableName);
        }
        catch (Exception e) {
            // todo add handle method to import client
            return null;
        }

        return new ImportTableHandle(tableName);
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle table)
    {
        return getTableMetadata(getTableName(table));
    }

    private TableMetadata getTableMetadata(QualifiedTableName tableName)
    {
        ImportClient client = importClientManager.getClient(tableName.getCatalogName());

        List<ColumnMetadata> columns = getTableSchema(client, tableName.getCatalogName(), tableName.getSchemaName(), tableName.getTableName());

        ImmutableList.Builder<String> partitionKeys = ImmutableList.builder();
        for (SchemaField partition : getPartitionKeys(client, tableName.getSchemaName(), tableName.getTableName())) {
            partitionKeys.add(partition.getFieldName());
        }

        return new TableMetadata(tableName, columns, partitionKeys.build());
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        if (!importClientManager.hasCatalog(prefix.getCatalogName())) {
            return ImmutableList.of();
        }

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
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        QualifiedTableName tableName = getTableName(tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata column : getTableMetadata(tableName).getColumns()) {
            builder.put(column.getName(), new ImportColumnHandle(tableName.getCatalogName(),
                    column.getName(),
                    column.getOrdinalPosition(),
                    column.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        QualifiedTableName tableName = getTableName(tableHandle);
        for (ColumnMetadata column : getTableMetadata(tableName).getColumns()) {
            if (column.getName().equals(columnName)) {
                return new ImportColumnHandle(tableName.getCatalogName(),
                        column.getName(),
                        column.getOrdinalPosition(),
                        column.getType());
            }
        }
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        QualifiedTableName tableName = getTableName(tableHandle);

        checkArgument(columnHandle instanceof InternalColumnHandle, "columnHandle is not an instance of InternalColumnHandle");
        InternalColumnHandle internalColumnHandle = (InternalColumnHandle) columnHandle;

        for (ColumnMetadata column : getTableMetadata(tableName).getColumns()) {
            if (internalColumnHandle.getColumnIndex() == column.getOrdinalPosition()) {
                return column;
            }
        }
        throw new IllegalArgumentException("Invalid column " + columnHandle);
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        ImmutableMap.Builder<QualifiedTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (QualifiedTableName tableName : listTables(prefix)) {
            builder.put(tableName, getTableMetadata(tableName).getColumns());
        }
        return builder.build();
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

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    private QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(DataSourceType.IMPORT == tableHandle.getDataSourceType(), "not a import handle: %s", tableHandle);

        ImportTableHandle importTableHandle = (ImportTableHandle) tableHandle;

        return importTableHandle.getTableName();
    }

    private static List<ColumnMetadata> getTableSchema(final ImportClient client, String catalogName, final String database, final String table)
    {
        return convertToMetadata(catalogName, retry()
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
                }));
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
