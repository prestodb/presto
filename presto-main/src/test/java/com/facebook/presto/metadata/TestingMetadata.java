package com.facebook.presto.metadata;

import com.facebook.presto.aggregation.AverageAggregation;
import com.facebook.presto.aggregation.CountAggregation;
import com.facebook.presto.aggregation.LongSumAggregation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TestingMetadata
        implements Metadata
{
    public static final Map<String, FunctionInfo> STANDARD_FUNCTIONS = ImmutableMap.<String, FunctionInfo>builder()
            .put("count", new FunctionInfo(true, CountAggregation.PROVIDER))
            .put("sum", new FunctionInfo(true, LongSumAggregation.PROVIDER))
            .put("avg", new FunctionInfo(true, AverageAggregation.PROVIDER))
            .build();

    private final Map<List<String>, TableMetadata> tables = new HashMap<>();
    private final Map<String, FunctionInfo> functions;

    public TestingMetadata()
    {
        this(STANDARD_FUNCTIONS);
    }

    public TestingMetadata(Map<String, FunctionInfo> functions)
    {
        this.functions = ImmutableMap.copyOf(checkNotNull(functions, "functions is null"));
    }

    @Override
    public FunctionInfo getFunction(String name)
    {
        checkArgument(name.equals(name.toLowerCase()), "name is not lowercase");
        FunctionInfo functionInfo = functions.get(name);
        checkArgument(functionInfo != null, "Function '%s' not defined", name);
        return functionInfo;
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkArgument(catalogName.equals(catalogName.toLowerCase()), "catalogName is not lowercase");
        checkArgument(schemaName.equals(schemaName.toLowerCase()), "schemaName is not lowercase");
        checkArgument(tableName.equals(tableName.toLowerCase()), "tableName is not lowercase");

        List<String> key = tableKey(catalogName, schemaName, tableName);
        TableMetadata table = tables.get(key);
        checkArgument(table != null, "Table '%s.%s.%s' not defined", catalogName, schemaName, tableName);
        return table;
    }

    @Override
    public synchronized void createTable(TableMetadata table)
    {
        List<String> key = tableKey(table);
        checkArgument(!tables.containsKey(key), "Table '%s.%s.%s' already defined",
                table.getSchemaName(), table.getCatalogName(), table.getTableName());
        tables.put(key, table);
    }

    private static List<String> tableKey(TableMetadata table)
    {
        return tableKey(table.getCatalogName(), table.getSchemaName(), table.getTableName());
    }

    private static List<String> tableKey(String catalogName, String schemaName, String tableName)
    {
        return ImmutableList.of(catalogName, schemaName, tableName);
    }
}
