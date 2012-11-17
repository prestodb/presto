package com.facebook.presto.metadata;

import com.facebook.presto.ingest.ImportSchemaUtil;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaField;
import com.facebook.presto.split.ImportClientFactory;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;
import java.util.concurrent.Callable;

import static com.google.common.base.Preconditions.checkArgument;
import static com.facebook.presto.util.RetryDriver.runWithRetryUnchecked;
import static com.google.common.base.Preconditions.checkNotNull;

public class ImportMetadata
        implements Metadata
{
    private final ImportClientFactory importClientFactory;

    @Inject
    public ImportMetadata(ImportClientFactory importClientFactory)
    {
        this.importClientFactory = checkNotNull(importClientFactory, "importClientFactory is null");
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableMetadata getTable(String catalogName, final String schemaName, final String tableName)
    {
        checkNotNull(catalogName, "catalogName is null");
        checkNotNull(schemaName, "schemaName is null");
        checkNotNull(tableName, "tableName is null");

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

        List<ColumnMetadata> columns = convertToMetadata(catalogName, importTableHandle, tableSchema);

        return new TableMetadata(catalogName, schemaName, tableName, columns, importTableHandle);
    }

    @Override
    public TableMetadata getTable(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof ImportTableHandle, "tableHandle must be of type ImportTableHandle, not %s", tableHandle.getClass().getName());
        assert tableHandle instanceof ImportTableHandle; // // IDEA-60343
        ImportTableHandle importTableHandle = (ImportTableHandle) tableHandle;

        return getTable(importTableHandle.getSourceName(), importTableHandle.getDatabaseName(), importTableHandle.getTableName());
    }

    private List<ColumnMetadata> convertToMetadata(final String sourceName, final ImportTableHandle importTableHandle, List<SchemaField> schemaFields)
    {
        return Lists.transform(schemaFields, new Function<SchemaField, ColumnMetadata>()
        {
            @Override
            public ColumnMetadata apply(SchemaField schemaField)
            {
                return new ColumnMetadata(
                        schemaField.getFieldName(),
                        ImportSchemaUtil.getTupleType(schemaField.getPrimitiveType()),
                        new ImportColumnHandle(sourceName, schemaField.getFieldName(), importTableHandle)
                );
            }
        });
    }

    @Override
    public ColumnMetadata getColumn(ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkArgument(columnHandle instanceof ImportColumnHandle, "columnHandle must be of type ImportColumnHandle, not %s", columnHandle.getClass().getName());
        assert columnHandle instanceof ImportColumnHandle; // // IDEA-60343

        ImportColumnHandle importColumnHandle = (ImportColumnHandle) columnHandle;
        TableMetadata table = getTable(importColumnHandle.getImportTableHandle());

        for (ColumnMetadata columnMetadata : table.getColumns()) {
            if (columnMetadata.getName().equals(importColumnHandle.getColumnName())) {
                return columnMetadata;
            }
        }

        throw new IllegalArgumentException("Unknown column handle name: " + importColumnHandle.getColumnName());
    }

    @Override
    public void createTable(TableMetadata table)
    {
        throw new UnsupportedOperationException();
    }
}
