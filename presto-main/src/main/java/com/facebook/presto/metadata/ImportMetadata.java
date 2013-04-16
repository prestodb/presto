package com.facebook.presto.metadata;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.SchemaTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Maps.EntryTransformer;
import com.google.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import static com.facebook.presto.util.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;

public class ImportMetadata
        implements ConnectorMetadata
{
    private final String clientId;
    private final ImportClient client;

    @Inject
    public ImportMetadata(String clientId, ImportClient client)
    {
        this.clientId = checkNotNull(clientId, "clientId is null");
        this.client = checkNotNull(client, "client is null");
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof ImportTableHandle && ((ImportTableHandle) tableHandle).getClientId().equals(clientId);
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
                return client.listSchemaNames();
            }
        });
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        checkNotNull(tableName, "tableName is null");
        TableHandle tableHandle = client.getTableHandle(tableName);
        if (tableHandle == null) {
            return null;
        }
        return new ImportTableHandle(clientId,
                new QualifiedTableName(clientId, tableName.getSchemaName(), tableName.getTableName()),
                tableHandle);
    }

    @Override
    public SchemaTableMetadata getTableMetadata(TableHandle table)
    {
        checkNotNull(table, "table is null");
        return client.getTableMetadata(getClientTableHandle(table));
    }

    @Override
    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        checkNotNull(schemaName, "schemaName is null");

        return client.listTables(schemaName.orNull());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        Map<String, ColumnHandle> columnHandles = client.getColumnHandles(getClientTableHandle(tableHandle));

        final ImmutableMap<String, ColumnMetadata> index = indexColumnMetadata(tableHandle);
        return ImmutableMap.copyOf(Maps.transformEntries(columnHandles, new EntryTransformer<String, ColumnHandle, ColumnHandle>()
        {
            @Override
            public ColumnHandle transformEntry(String columnName, ColumnHandle columnHandle)
            {
                return wrapClientHandle(columnName, columnHandle, index);
            }
        }));
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        ColumnHandle columnHandle = client.getColumnHandle(getClientTableHandle(tableHandle), columnName);
        if (columnHandle == null) {
            return null;
        }
        return wrapClientHandle(columnName, columnHandle, indexColumnMetadata(tableHandle));
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        return client.getColumnMetadata(getClientTableHandle(tableHandle), getClientColumnHandle(columnHandle));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        return client.listTableColumns(prefix);
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(SchemaTablePrefix prefix)
    {
        return client.listTablePartitionValues(prefix);
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

    private static TableHandle getClientTableHandle(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkState(tableHandle instanceof ImportTableHandle, "not a import handle: %s", tableHandle);
        ImportTableHandle importTableHandle = (ImportTableHandle) tableHandle;
        return importTableHandle.getTableHandle();
    }

    private static ColumnHandle getClientColumnHandle(ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle, "columnHandle is null");
        checkState(columnHandle instanceof ImportColumnHandle, "not a import handle: %s", columnHandle);
        ImportColumnHandle importColumnHandle = (ImportColumnHandle) columnHandle;
        return importColumnHandle.getColumnHandle();
    }

    private ImmutableMap<String, ColumnMetadata> indexColumnMetadata(TableHandle tableHandle)
    {
        return uniqueIndex(getTableMetadata(tableHandle).getColumns(), columnNameGetter());
    }

    private static ColumnHandle wrapClientHandle(String columnName, ColumnHandle columnHandle, ImmutableMap<String,ColumnMetadata> metadata)
    {
        ColumnMetadata columnMetadata = metadata.get(columnName);
        checkState(columnMetadata != null, "Can not find metadata for column %s", columnName);
        return new ImportColumnHandle(columnMetadata.getName(), columnMetadata.getOrdinalPosition(), columnMetadata.getType(), columnHandle);
    }

    private static Function<ColumnMetadata, String> columnNameGetter()
    {
        return new Function<ColumnMetadata, String>() {

            @Override
            public String apply(ColumnMetadata input)
            {
                return input.getName();
            }
        };
    }
}
