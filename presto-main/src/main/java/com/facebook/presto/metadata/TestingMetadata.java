package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.checkTable;
import static com.facebook.presto.metadata.MetadataUtil.checkTableName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Maps.filterKeys;

public class TestingMetadata
        extends AbstractMetadata
{
    private final Map<QualifiedTableName, TableMetadata> tables = new HashMap<>();
    private final FunctionRegistry functions = new FunctionRegistry();

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        return functions.get(name, parameterTypes);
    }

    @Override
    public FunctionInfo getFunction(FunctionHandle handle)
    {
        return functions.get(handle);
    }

    @Override
    public List<FunctionInfo> listFunctions()
    {
        return functions.list();
    }

    @Override
    public List<String> listSchemaNames(String catalogName)
    {
        List<QualifiedTableName> tables = listTables(catalogName, Optional.<String>absent());
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
    public List<TableColumn> listTableColumns(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        return getTableColumns(matches(catalogName, schemaName, tableName));
    }


    @Override
    public List<String> listTablePartitionKeys(QualifiedTableName table)
    {
        return ImmutableList.of();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(String catalogName, Optional<String> schemaName, Optional<String> tableName)
    {
        return ImmutableList.of();
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, Optional<String> schemaName)
    {
        ImmutableList.Builder<QualifiedTableName> builder = ImmutableList.builder();
        for (QualifiedTableName name : tables.keySet()) {
            if (!schemaName.isPresent() || schemaName.get().equals(name.getSchemaName())) {
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

    private List<TableColumn> getTableColumns(Predicate<QualifiedTableName> predicate)
    {
        Iterable<TableMetadata> values = filterKeys(tables, predicate).values();
        return ImmutableList.copyOf(concat(transform(values, toTableColumns())));
    }

    private static Predicate<QualifiedTableName> matches(final String catalogName, final Optional<String> schemaName, final Optional<String> tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        return new Predicate<QualifiedTableName>()
        {
            @Override
            public boolean apply(QualifiedTableName key)
            {
                return catalogName.equals(key.getCatalogName())
                        && (!schemaName.isPresent() || schemaName.equals(key.getSchemaName()))
                        && (!tableName.isPresent() || tableName.equals(key.getTableName()));
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
