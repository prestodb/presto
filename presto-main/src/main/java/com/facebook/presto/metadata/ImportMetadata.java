package com.facebook.presto.metadata;

import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;
import java.util.concurrent.Callable;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.facebook.presto.util.RetryDriver.runWithRetryUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportMetadata
        extends AbstractMetadata
{
    private final ImportClientFactory importClientFactory;

    @Inject
    public ImportMetadata(ImportClientFactory importClientFactory)
    {
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
    }

    @Override
    public TableMetadata getTable(String catalogName, final String schemaName, final String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);

        final ImportClient client = importClientFactory.getClient(catalogName);
        List<SchemaField> tableSchema = runWithRetryUnchecked(new Callable<List<SchemaField>>()
        {
            @Override
            public List<SchemaField> call()
                    throws Exception
            {
                return client.getTableSchema(schemaName, tableName);
            }
        });

        ImportTableHandle importTableHandle = new ImportTableHandle(catalogName, schemaName, tableName);

        List<ColumnMetadata> columns = convertToMetadata(catalogName, tableSchema);

        return new TableMetadata(catalogName, schemaName, tableName, columns, importTableHandle);
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName)
    {
        checkCatalogName(catalogName);

        ImportClient client = importClientFactory.getClient(catalogName);

        ImmutableList.Builder<QualifiedTableName> list = ImmutableList.builder();

        List<String> schemas = getDatabaseNames(client);
        for (String schema : schemas) {
            List<String> tables = getTableNames(client, schema);
            for (String table : tables) {
                list.add(new QualifiedTableName(catalogName, schema, table));
            }
        }

        return list.build();
    }

    private static List<String> getTableNames(final ImportClient client, final String database)
    {
        return runWithRetryUnchecked(new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return client.getTableNames(database);
            }
        });
    }

    private static List<String> getDatabaseNames(final ImportClient client)
    {
        return runWithRetryUnchecked(new Callable<List<String>>()
        {
            @Override
            public List<String> call()
                    throws Exception
            {
                return client.getDatabaseNames();
            }
        });
    }

    private List<ColumnMetadata> convertToMetadata(final String sourceName, List<SchemaField> schemaFields)
    {
        return Lists.transform(schemaFields, new Function<SchemaField, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(SchemaField schemaField)
            {
                TupleInfo.Type type = ImportSchemaUtil.getTupleType(schemaField.getPrimitiveType());
                ImportColumnHandle columnHandle = new ImportColumnHandle(sourceName, schemaField.getFieldName(), schemaField.getFieldId(), type);
                return new ColumnMetadata(schemaField.getFieldName(), type, columnHandle);
            }
        });
    }
}
