package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class Metadata
{
    private final Map<QualifiedName, TableMetadata> tables;
    private final Map<QualifiedName, FunctionInfo> functions;

    public Metadata(Map<QualifiedName, TableMetadata> tables, Map<QualifiedName, FunctionInfo> functions)
    {
        Preconditions.checkNotNull(tables, "tables is null");
        Preconditions.checkNotNull(functions, "functions is null");

        this.tables = ImmutableMap.copyOf(tables);
        this.functions = ImmutableMap.copyOf(functions);
    }

    public FunctionInfo getFunction(QualifiedName name)
    {
        FunctionInfo functionInfo = functions.get(name);

        Preconditions.checkArgument(functionInfo != null, "Function '%s' not defined", name);

        return functionInfo; // TODO
    }

    public TableMetadata getTable(QualifiedName name)
    {
        TableMetadata table = tables.get(name);

        Preconditions.checkArgument(table != null, "Table '%s' not defined", name);

        return table; // TODO
    }
}
