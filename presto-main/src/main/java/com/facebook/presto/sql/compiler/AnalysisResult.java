package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.IdentityHashMap;

public class AnalysisResult
{
    private final Schema type;
    private final IdentityHashMap<QualifiedNameReference, QualifiedName> resolvedNames;
    private final IdentityHashMap<Node, Schema> types;

    public AnalysisResult(Schema type, IdentityHashMap<QualifiedNameReference, QualifiedName> resolvedNames, IdentityHashMap<Node, Schema> types)
    {
        this.type = type;
        this.resolvedNames = resolvedNames;
        this.types = types;
    }

    public Schema getType()
    {
        return type;
    }

    public IdentityHashMap<QualifiedNameReference, QualifiedName> getResolvedNames()
    {
        return resolvedNames;
    }

    public IdentityHashMap<Node, Schema> getTypes()
    {
        return types;
    }
}
