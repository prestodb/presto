package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.inject.Inject;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportMetadata
        implements ConnectorMetadata
{
    private final ImportClient client;

    @Inject
    public ImportMetadata(ImportClient client)
    {
        this.client = checkNotNull(client, "client is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return client.canHandle(tableHandle);
    }

    @Override
    public List<String> listSchemaNames()
    {
        return client.listSchemaNames();
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        return client.getTableHandle(tableName);
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle table)
    {
        return client.getTableMetadata(table);
    }

    @Override
    public List<SchemaTableName> listTables(@Nullable String schemaNameOrNull)
    {
        return client.listTables(schemaNameOrNull);
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        return client.getColumnHandles(tableHandle);
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        return client.getColumnHandle(tableHandle, columnName);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return client.getColumnMetadata(tableHandle, columnHandle);
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        return client.listTableColumns(prefix);
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
}
