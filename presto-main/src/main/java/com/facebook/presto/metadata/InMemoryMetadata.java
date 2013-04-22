package com.facebook.presto.metadata;

import com.facebook.presto.tpch.TpchColumnHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InMemoryMetadata
        implements ConnectorMetadata
{
    private final ConcurrentMap<QualifiedTableName, TableMetadata> tables = new ConcurrentHashMap<>();

    @Override
    public int priority()
    {
        return Integer.MIN_VALUE;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return tableHandle instanceof InMemoryTableHandle;
    }

    @Override
    public boolean canHandle(QualifiedTablePrefix prefix)
    {
        return prefix.getCatalogName().equals("tpch");
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        checkNotNull(catalogName, "catalogName is null");

        List<QualifiedTableName> tables = listTables(QualifiedTablePrefix.builder(catalogName).build());
        Set<String> schemaNames = new HashSet<>();

        for (QualifiedTableName qualifiedTableName : tables) {
            schemaNames.add(qualifiedTableName.getSchemaName());
        }

        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public TableHandle getTableHandle(QualifiedTableName tableName)
    {
        checkTable(tableName);
        if (!tables.containsKey(tableName)) {
            return null;
        }
        return new InMemoryTableHandle(tableName);
    }

    @Override
    public TableMetadata getTableMetadata(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        QualifiedTableName tableName = getTableName(tableHandle);
        TableMetadata tableMetadata = tables.get(tableName);
        checkArgument(tableMetadata != null, "Table %s does not exist", tableName);
        return tableMetadata;
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(TableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> builder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            builder.put(columnMetadata.getName(), new TpchColumnHandle(columnMetadata.getOrdinalPosition(), columnMetadata.getType()));
        }
        return builder.build();
    }

    @Override
    public ColumnHandle getColumnHandle(TableHandle tableHandle, String columnName)
    {
        for (ColumnMetadata columnMetadata : getTableMetadata(tableHandle).getColumns()) {
            if (columnMetadata.getName().equals(columnName)) {
                return new TpchColumnHandle(columnMetadata.getOrdinalPosition(), columnMetadata.getType());
            }
        }
        return null;
    }

    @Override
    public Map<QualifiedTableName, List<ColumnMetadata>> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableMap.Builder<QualifiedTableName, List<ColumnMetadata>> tableColumns = ImmutableMap.builder();
        for (QualifiedTableName tableName : listTables(prefix)) {
            int position = 1;
            ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
            for (ColumnMetadata column : tables.get(tableName).getColumns()) {
                columns.add(new ColumnMetadata(column.getName(), column.getType(), position));
                position++;
            }
            tableColumns.put(tableName, columns.build());
        }
        return tableColumns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        QualifiedTableName tableName = getTableName(tableHandle);
        checkArgument(columnHandle instanceof TpchColumnHandle, "columnHandle is not an instance of TpchColumnHandle");
        TpchColumnHandle tpchColumnHandle = (TpchColumnHandle) columnHandle;
        int columnIndex = tpchColumnHandle.getFieldIndex();
        return tables.get(tableName).getColumns().get(columnIndex);
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        return ImmutableList.of();
    }

    @Override
    public List<QualifiedTableName> listTables(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        ImmutableList.Builder<QualifiedTableName> builder = ImmutableList.builder();
        for (QualifiedTableName tableName : tables.keySet()) {
            if (prefix.getCatalogName().equals(tableName.getCatalogName()) &&
                    prefix.getSchemaName().isPresent() || prefix.getSchemaName().get().equals(tableName.getSchemaName()) &&
                    prefix.getTableName().isPresent() || prefix.getTableName().get().equals(tableName.getTableName())) {
                builder.add(tableName);
            }
        }
        return builder.build();
    }

    @Override
    public TableHandle createTable(TableMetadata tableMetadata)
    {
        TableMetadata existingTable = tables.putIfAbsent(tableMetadata.getTable(), tableMetadata);
        checkArgument(existingTable == null, "Table %s already exists", tableMetadata.getTable());
        return new InMemoryTableHandle(tableMetadata.getTable());
    }

    @Override
    public void dropTable(TableHandle tableHandle)
    {
        tables.remove(getTableName(tableHandle));
    }

    private QualifiedTableName getTableName(TableHandle tableHandle)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkArgument(tableHandle instanceof InMemoryTableHandle, "tableHandle is not an instance of InMemoryTableHandle");
        InMemoryTableHandle inMemoryTableHandle = (InMemoryTableHandle) tableHandle;
        return inMemoryTableHandle.getTableName();
    }

    public static class InMemoryTableHandle
            implements TableHandle
    {
        private final QualifiedTableName tableName;

        public InMemoryTableHandle(QualifiedTableName qualifiedTableName)
        {
            this.tableName = qualifiedTableName;
        }

        @Override
        public DataSourceType getDataSourceType()
        {
            return DataSourceType.INTERNAL;
        }

        public QualifiedTableName getTableName()
        {
            return tableName;
        }
    }
}
