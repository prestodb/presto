package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TestingMetadata
        implements Metadata
{
    private final Map<List<String>, TableMetadata> tables;
    private final Map<String, FunctionInfo> functions;

    public TestingMetadata(List<TableMetadata> tables, Map<String, FunctionInfo> functions)
    {
        this.tables = tableMap(checkNotNull(tables, "tables is null"));
        this.functions = ImmutableMap.copyOf(checkNotNull(functions, "functions is null"));
    }

    @Override
    public FunctionInfo getFunction(String name)
    {
        FunctionInfo functionInfo = functions.get(name);
        checkArgument(functionInfo != null, "Function '%s' not defined", name);
        return functionInfo;
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        List<String> key = tableKey(catalogName, schemaName, tableName);
        TableMetadata table = tables.get(key);
        checkArgument(table != null, "Table '%s.%s.%s' not defined", catalogName, schemaName, tableName);
        return table;
    }

    private static Map<List<String>, TableMetadata> tableMap(List<TableMetadata> tables)
    {
        ImmutableMap.Builder<List<String>, TableMetadata> map = ImmutableMap.builder();
        for (TableMetadata table : tables) {
            List<String> key = tableKey(table.getCatalogName(), table.getSchemaName(), table.getTableName());
            map.put(key, table);
        }
        return map.build();
    }

    private static List<String> tableKey(String catalogName, String schemaName, String tableName)
    {
        return ImmutableList.of(catalogName, schemaName, tableName);
    }
}
