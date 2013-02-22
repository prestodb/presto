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
import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.metadata.MetadataUtil.getTableColumns;
import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;

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
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        ImportClient client = importClientManager.getClient(catalogName);

        List<SchemaField> tableSchema = getTableSchema(client, schemaName, tableName);

        ImportTableHandle importTableHandle = new ImportTableHandle(catalogName, schemaName, tableName);

        List<ColumnMetadata> columns = convertToMetadata(catalogName, tableSchema);

        return new TableMetadata(catalogName, schemaName, tableName, columns, importTableHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName)
    {
        checkCatalogName(catalogName);
        ImportClient client = importClientManager.getClient(catalogName);

        ImmutableList.Builder<QualifiedTableName> list = ImmutableList.builder();
        for (String schema : getDatabaseNames(client)) {
            List<String> tables = getTableNames(client, schema);
            for (String table : tables) {
                list.add(new QualifiedTableName(catalogName, schema, table));
            }
        }
        return list.build();
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, String schemaName)
    {
        checkSchemaName(catalogName, schemaName);
        ImportClient client = importClientManager.getClient(catalogName);

        ImmutableList.Builder<QualifiedTableName> list = ImmutableList.builder();
        for (String table : getTableNames(client, schemaName)) {
            list.add(new QualifiedTableName(catalogName, schemaName, table));
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
    public List<TableColumn> listTableColumns(String catalogName)
    {
        checkCatalogName(catalogName);
        ImportClient client = importClientManager.getClient(catalogName);

        ImmutableList.Builder<TableColumn> list = ImmutableList.builder();
        for (String schema : getDatabaseNames(client)) {
            list.addAll(listTableColumns(catalogName, schema));
        }
        return list.build();
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName)
    {
        checkSchemaName(catalogName, schemaName);
        ImportClient client = importClientManager.getClient(catalogName);

        ImmutableList.Builder<TableColumn> list = ImmutableList.builder();
        for (String table : getTableNames(client, schemaName)) {
            list.addAll(listTableColumns(catalogName, schemaName, table));
        }
        return list.build();
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        ImportClient client = importClientManager.getClient(catalogName);

        List<SchemaField> tableSchema = getTableSchema(client, schemaName, tableName);
        Map<String, List<ColumnMetadata>> map = ImmutableMap.of(tableName, convertToMetadata(catalogName, tableSchema));
        return getTableColumns(catalogName, schemaName, map);
    }

    @Override
    public List<String> listTablePartitionKeys(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        ImportClient client = importClientManager.getClient(catalogName);

        ImmutableList.Builder<String> list = ImmutableList.builder();
        for (SchemaField partition : getPartitionKeys(client, schemaName, tableName)) {
            list.add(partition.getFieldName());
        }
        return list.build();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        ImportClient client = importClientManager.getClient(catalogName);

        ImmutableList.Builder<Map<String, String>> list = ImmutableList.builder();
        for (PartitionInfo partition : getPartitions(client, schemaName, tableName)) {
            list.add(partition.getKeyFields());
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
