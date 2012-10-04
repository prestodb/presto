package com.facebook.presto.metadata;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class Metadata
{
    private final Map<String, TableMetadata> tables;
    private final Map<String, FunctionInfo> functions;

    public Metadata(Map<String, TableMetadata> tables, Map<String, FunctionInfo> functions)
    {
        Preconditions.checkNotNull(tables, "tables is null");
        Preconditions.checkNotNull(functions, "functions is null");

        this.tables = ImmutableMap.copyOf(tables);
        this.functions = ImmutableMap.copyOf(functions);
    }

    public FunctionInfo getFunction(QualifiedName name)
    {
        return functions.get(name.getParts().get(0).toUpperCase()); // TODO
    }

    public TableMetadata getTable(QualifiedName name)
    {
        return tables.get(name.getParts().get(0).toUpperCase()); // TODO
    }
}
