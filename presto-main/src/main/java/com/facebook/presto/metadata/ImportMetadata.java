package com.facebook.presto.metadata;

import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.ObjectNotFoundException;
import com.facebook.presto.spi.PartitionInfo;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class ImportMetadata
        implements ConnectorMetadata
{
    private final String catalog;
    private final ImportClient client;

    @Inject
    public ImportMetadata(String catalog, ImportClient client)
    {
        this.catalog = checkNotNull(catalog, "catalog is null");
        this.client = checkNotNull(client, "client is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof ImportTableHandle;
    }

    @Override
    public List<String> listSchemaNames()
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

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        // verify table exists
        try {
            getTableMetadata(tableName);
        }
        catch (Exception e) {
            // todo add handle method to import client
            return null;
        }

        return new ImportTableHandle(new QualifiedTableName(catalog, tableName.getSchemaName(), tableName.getTableName()));
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle table)
    {
        return getTableMetadata(getTableName(table));
    }

    private SchemaTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        List<ColumnMetadata> columns = getTableSchema(client, tableName.getSchemaName(), tableName.getTableName());

        ImmutableList.Builder<String> partitionKeys = ImmutableList.builder();
        for (SchemaField partition : getPartitionKeys(client, tableName.getSchemaName(), tableName.getTableName())) {
            partitionKeys.add(partition.getFieldName());
        }

        return new SchemaTableMetadata(tableName, columns, partitionKeys.build());
    }

    @Override
    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        checkNotNull(schemaName, "schemaName is null");

        Iterable<String> schemaNames = schemaName.isPresent() ? schemaName.asSet() : listSchemaNames();

        ImmutableList.Builder<SchemaTableName> tables = ImmutableList.builder();
        for (String schema : schemaNames) {
            for (String table : getTableNames(client, schema)) {
                tables.add(new SchemaTableName(schema, table));
            }
        }
        return tables.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata column : getTableMetadata(tableName).getColumns()) {
            builder.put(column.getName(), new ImportColumnHandle(column.getName(),
                    column.getOrdinalPosition(),
                    column.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        SchemaTableName tableName = getTableName(tableHandle);
        for (ColumnMetadata column : getTableMetadata(tableName).getColumns()) {
            if (column.getName().equals(columnName)) {
                return new ImportColumnHandle(column.getName(),
                        column.getOrdinalPosition(),
                        column.getType());
            }
        }
        return null;
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        SchemaTableName tableName = getTableName(tableHandle);

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
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(prefix.getSchemaName())) {
            builder.put(tableName, getTableMetadata(tableName).getColumns());
        }
        return builder.build();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        Iterable<String> databaseNames = prefix.getSchemaName().isPresent() ? prefix.getSchemaName().asSet() : listSchemaNames();

        ImmutableList.Builder<Map<String, String>> list = ImmutableList.builder();

        for (String databaseName : databaseNames) {
            Iterable<String> tableNames = prefix.getTableName().isPresent() ? prefix.getTableName().asSet() : getTableNames(client, databaseName);
            for (String tblName : tableNames) {
                for (PartitionInfo partition : getPartitions(client, databaseName, tblName)) {
                    list.add(partition.getKeyFields());
                }
            }
        }
        return list.build();
    }

    @Override
    public TableHandle createTable(SchemaTableMetadata tableMetadata)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    private SchemaTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(DataSourceType.IMPORT == tableHandle.getDataSourceType(), "not a import handle: %s", tableHandle);

        ImportTableHandle importTableHandle = (ImportTableHandle) tableHandle;

        return importTableHandle.getTableName().asSchemaTableName();
    }

    private static List<ColumnMetadata> getTableSchema(final ImportClient client, final String database, final String table)
    {
        return retry()
                .stopOn(ObjectNotFoundException.class)
                .stopOnIllegalExceptions()
                .runUnchecked(new Callable<List<ColumnMetadata>>()
                {
                    @Override
                    public List<ColumnMetadata> call()
                            throws Exception
                    {
                        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();
                        for (SchemaField schemaField : client.getTableSchema(database, table)) {
                            Type type;
                            switch (schemaField.getPrimitiveType()) {
                                case LONG:
                                    type = TupleInfo.Type.FIXED_INT_64;
                                    break;
                                case DOUBLE:
                                    type = TupleInfo.Type.DOUBLE;
                                    break;
                                case STRING:
                                    type = TupleInfo.Type.VARIABLE_BINARY;
                                    break;
                                default:
                                    throw new IllegalArgumentException("Unhandled type: " + schemaField.getPrimitiveType());
                            }
                            builder.add(new ColumnMetadata(schemaField.getFieldName(), type, schemaField.getFieldId()));
                        }
                        return builder.build();
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
