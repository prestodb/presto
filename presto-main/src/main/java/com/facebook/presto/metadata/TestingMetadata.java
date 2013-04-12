package com.facebook.presto.metadata;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;

public class TestingMetadata
        implements ConnectorMetadata
{
    private final Map<QualifiedTableName, TableMetadata> tables = new HashMap<>();

    @Override
    public int priority()
    {
        return Integer.MIN_VALUE;
    }

    @Override
    public boolean canHandle(TableHandle tableHandle)
    {
        return false;
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
    public TableMetadata getTable(QualifiedTableName table)
    {
        checkTable(table);
        return tables.get(table);
    }

    @Override
    public List<TableColumn> listTableColumns(QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        return getTableColumns(matches(prefix));
    }


    @Override
    public List<String> listTablePartitionKeys(QualifiedTableName table)
    {
        return ImmutableList.of();
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
        for (QualifiedTableName name : tables.keySet()) {
            if (prefix.matches(name)) {
                builder.add(name);
            }
        }
        return builder.build();
    }

    @Override
    public synchronized void createTable(TableMetadata table)
    {
        QualifiedTableName key = table.getTable();
        checkArgument(!tables.containsKey(key), "Table '%s' already defined", key);
        tables.put(key, table);
    }

    @Override
    public synchronized void dropTable(TableMetadata table)
    {
        QualifiedTableName key = table.getTable();
        checkArgument(tables.containsKey(key), "Table '%s' not defined", key);
        tables.remove(key);
    }

    public QualifiedTableName getTableName(TableHandle tableHandle)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TableColumn getTableColumn(TableHandle tableHandle, ColumnHandle columnHandle)
    {
        throw new UnsupportedOperationException();
    }

    private List<TableColumn> getTableColumns(Predicate<QualifiedTableName> predicate)
    {
        Iterable<TableMetadata> values = filterKeys(tables, predicate).values();
        return ImmutableList.copyOf(concat(transform(values, toTableColumns())));
    }

    private static Predicate<QualifiedTableName> matches(final QualifiedTablePrefix prefix)
    {
        checkNotNull(prefix, "prefix is null");

        return new Predicate<QualifiedTableName>()
        {
            @Override
            public boolean apply(QualifiedTableName key)
            {
                return prefix.getCatalogName().equals(key.getCatalogName())
                        && (!prefix.hasSchemaName() || prefix.getSchemaName().get().equals(key.getSchemaName()))
                        && (!prefix.hasTableName() || prefix.getTableName().get().equals(key.getTableName()));
            }
        };
    }

    private static Function<TableMetadata, List<TableColumn>> toTableColumns()
    {
        return new Function<TableMetadata, List<TableColumn>>()
        {
            @Override
            public List<TableColumn> apply(TableMetadata input)
            {
                ImmutableList.Builder<TableColumn> columns = ImmutableList.builder();
                int position = 1;
                for (ColumnMetadata column : input.getColumns()) {
                    columns.add(new TableColumn(input.getTable(),
                            column.getName(), position, column.getType()));
                    position++;
                }
                return columns.build();
            }
        };
    }
}
