package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class TestingMetadata
        implements Metadata
{
    private final Map<List<String>, TableMetadata> tables = new HashMap<>();
    private final Multimap<QualifiedName, FunctionInfo> functions;

    public TestingMetadata()
    {
        functions = buildFunctions(
                new FunctionInfo(QualifiedName.of("count"), true, TupleInfo.Type.FIXED_INT_64, ImmutableList.<TupleInfo.Type>of(), null, null),
                new FunctionInfo(QualifiedName.of("sum"), true, TupleInfo.Type.FIXED_INT_64, ImmutableList.of(TupleInfo.Type.FIXED_INT_64), null, null),
                new FunctionInfo(QualifiedName.of("sum"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.DOUBLE), null, null),
                new FunctionInfo(QualifiedName.of("avg"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.DOUBLE), null, null),
                new FunctionInfo(QualifiedName.of("avg"), true, TupleInfo.Type.DOUBLE, ImmutableList.of(TupleInfo.Type.FIXED_INT_64), null, null)
        );
    }

    private Multimap<QualifiedName, FunctionInfo> buildFunctions(FunctionInfo... infos)
    {
        return Multimaps.index(ImmutableList.copyOf(infos), new Function<FunctionInfo, QualifiedName>()
        {
            @Override
            public QualifiedName apply(FunctionInfo input)
            {
                return input.getName();
            }
        });
    }

    @Override
    public FunctionInfo getFunction(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        for (FunctionInfo functionInfo : functions.get(name)) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        throw new IllegalArgumentException(format("Function %s(%s) not registered", name, Joiner.on(", ").join(parameterTypes)));
    }

    @Override
    public TableMetadata getTable(String catalogName, String schemaName, String tableName)
    {
        checkArgument(catalogName.equals(catalogName.toLowerCase()), "catalogName is not lowercase");
        checkArgument(schemaName.equals(schemaName.toLowerCase()), "schemaName is not lowercase");
        checkArgument(tableName.equals(tableName.toLowerCase()), "tableName is not lowercase");

        List<String> key = tableKey(catalogName, schemaName, tableName);
        return tables.get(key);
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
