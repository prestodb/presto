package com.facebook.presto.connector.system;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.TableMetadata;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.connector.system.SystemColumnHandle.toSystemColumnHandles;
import static com.facebook.presto.metadata.MetadataUtil.findColumnMetadata;
import static com.facebook.presto.metadata.MetadataUtil.schemaNameGetter;
import static com.facebook.presto.metadata.MetadataUtil.tableNameGetter;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class SystemTablesMetadata
        implements ConnectorMetadata
{
    private final ConcurrentMap<SchemaTableName, TableMetadata> tables = new ConcurrentHashMap<>();

    public void addTable(TableMetadata tableMetadata)
    {
        checkArgument(tables.putIfAbsent(tableMetadata.getTable(), tableMetadata) == null, "Table %s is already registered", tableMetadata.getTable());
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof SystemTableHandle && tables.containsKey(((SystemTableHandle) tableHandle).getSchemaTableName());
    }

    private SystemTableHandle checkTableHandle(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof SystemTableHandle, "tableHandle is not a system table handle");
        SystemTableHandle systemTableHandle = (SystemTableHandle) tableHandle;
        checkArgument(tables.containsKey(systemTableHandle.getSchemaTableName()));
        return systemTableHandle;
    }

    @Override
    public List<String> listSchemaNames()
    {
        // remove duplicates
        ImmutableSet<String> schemaNames = ImmutableSet.copyOf(transform(tables.keySet(), tableNameGetter()));
        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public TableHandle getTableHandle(SchemaTableName tableName)
    {
        if (!tables.containsKey(tableName)) {
            return null;
        }
        return new SystemTableHandle(tableName);
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);
        return tables.get(systemTableHandle.getSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(final String schemaNameOrNull)
    {
        if (schemaNameOrNull == null) {
            return ImmutableList.copyOf(tables.keySet());
        }

        return ImmutableList.copyOf(filter(tables.keySet(), compose(equalTo(schemaNameOrNull), schemaNameGetter())));
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);
        TableMetadata tableMetadata = tables.get(systemTableHandle.getSchemaTableName());

        if (findColumnMetadata(tableMetadata, columnName) == null) {
            return null;
        }
        return new SystemColumnHandle(columnName);
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);
        TableMetadata tableMetadata = tables.get(systemTableHandle.getSchemaTableName());

        checkArgument(columnHandle instanceof SystemColumnHandle, "columnHandle is not an instance of SystemColumnHandle");
        String columnName = ((SystemColumnHandle) columnHandle).getColumnName();

        ColumnMetadata columnMetadata = findColumnMetadata(tableMetadata, columnName);
        checkArgument(columnMetadata != null, "Column %s on table %s does not exist", columnName, tableMetadata.getTable());
        return columnMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        SystemTableHandle systemTableHandle = checkTableHandle(tableHandle);

        return toSystemColumnHandles(tables.get(systemTableHandle.getSchemaTableName()));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(SchemaTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> builder = ImmutableMap.builder();
        for (Entry<SchemaTableName, TableMetadata> entry : tables.entrySet()) {
            if (prefix.matches(entry.getKey())) {
                builder.put(entry.getKey(), entry.getValue().getColumns());
            }
        }
        return builder.build();
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
}
