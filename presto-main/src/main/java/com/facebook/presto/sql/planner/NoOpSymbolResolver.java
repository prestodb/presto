package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.QualifiedNameReference;

public class NoOpSymbolResolver
    implements SymbolResolver
{
    public static final NoOpSymbolResolver INSTANCE = new NoOpSymbolResolver();

    @Override
    public Object getValue(Symbol symbol)
    {
        return new QualifiedNameReference(symbol.toQualifiedName());
    }
}
