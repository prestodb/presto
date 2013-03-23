package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.MetadataUtil.checkCatalogName;
import static com.facebook.presto.metadata.MetadataUtil.checkSchemaName;
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
        List<QualifiedTableName> tables = listTables(catalogName);
        Set<String> schemaNames = new HashSet<>();

        for (QualifiedTableName qualifiedTableName : tables) {
            schemaNames.add(qualifiedTableName.getSchemaName());
        }

        return ImmutableList.copyOf(schemaNames);
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        return tables.get(tableKey(catalogName, schemaName, tableName));
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName)
    {
        return getTableNames(catalogMatches(catalogName));
    }

    @Override
    public List<QualifiedTableName> listTables(String catalogName, String schemaName)
    {
        return getTableNames(schemaMatches(catalogName, schemaName));
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName)
    {
        return getTableColumns(catalogMatches(catalogName));
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName)
    {
        return getTableColumns(schemaMatches(catalogName, schemaName));
    }

    @Override
    public List<TableColumn> listTableColumns(String catalogName, String schemaName, String tableName)
    {
        return getTableColumns(tableMatches(catalogName, schemaName, tableName));
    }

    @Override
    public List<String> listTablePartitionKeys(String catalogName, String schemaName, String tableName)
    {
        return ImmutableList.of();
    }

    @Override
    public List<Map<String, String>> listTablePartitionValues(String catalogName, String schemaName, String tableName)
    {
        return ImmutableList.of();
    }

    @Override
    public synchronized void createTable(TableMetadata table)
    {
        QualifiedTableName key = tableKey(table);
        checkArgument(!tables.containsKey(key), "Table '%s.%s.%s' already defined",
                table.getSchemaName(), table.getCatalogName(), table.getTableName());
        tables.put(key, table);
    }

    private List<QualifiedTableName> getTableNames(Predicate<QualifiedTableName> predicate)
    {
        Iterable<TableMetadata> values = filterKeys(tables, predicate).values();
        return ImmutableList.copyOf(transform(values, toQualifiedTableName()));
    }

    private List<TableColumn> getTableColumns(Predicate<QualifiedTableName> predicate)
    {
        Iterable<TableMetadata> values = filterKeys(tables, predicate).values();
        return ImmutableList.copyOf(concat(transform(values, toTableColumns())));
    }

    private static Predicate<QualifiedTableName> catalogMatches(final String catalogName)
    {
        checkCatalogName(catalogName);
        return new Predicate<QualifiedTableName>()
        {
            @Override
            public boolean apply(QualifiedTableName key)
            {
                return key.getCatalogName().equals(catalogName);
            }
        };
    }

    private static Predicate<QualifiedTableName> schemaMatches(final String catalogName, final String schemaName)
    {
        checkSchemaName(catalogName, schemaName);
        return new Predicate<QualifiedTableName>()
        {
            @Override
            public boolean apply(QualifiedTableName key)
            {
                return key.getCatalogName().equals(catalogName) && key.getSchemaName().equals(schemaName);
            }
        };
    }

    private static Predicate<QualifiedTableName> tableMatches(final String catalogName, final String schemaName, final String tableName)
    {
        checkTableName(catalogName, schemaName, tableName);
        return new Predicate<QualifiedTableName>()
        {
            @Override
            public boolean apply(QualifiedTableName key)
            {
                return key.equals(new QualifiedTableName(catalogName, schemaName, tableName));
            }
        };
    }

    private static QualifiedTableName tableKey(TableMetadata table)
    {
        return tableKey(table.getCatalogName(), table.getSchemaName(), table.getTableName());
    }

    private static QualifiedTableName tableKey(String catalogName, String schemaName, String tableName)
    {
        return new QualifiedTableName(catalogName, schemaName, tableName);
    }

    private static Function<TableMetadata, QualifiedTableName> toQualifiedTableName()
    {
        return new Function<TableMetadata, QualifiedTableName>()
        {
            @Override
            public QualifiedTableName apply(TableMetadata input)
            {
                return new QualifiedTableName(input.getCatalogName(), input.getSchemaName(), input.getTableName());
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
                    columns.add(new TableColumn(
                            input.getCatalogName(), input.getSchemaName(), input.getTableName(),
                            column.getName(), position, column.getType()));
                    position++;
                }
                return columns.build();
            }
        };
    }
}
