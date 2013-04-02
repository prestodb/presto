package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;

import java.util.Map;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkNotNull;

public class LookupSymbolResolver
        implements SymbolResolver
{
    private final Map<Symbol, Object> bindings;

    public LookupSymbolResolver(Map<Symbol, Object> bindings)
    {
        checkNotNull(bindings, "bindings is null");

        this.bindings = ImmutableMap.copyOf(bindings);
    }

    public Object getValue(Symbol symbol)
    {
        if (!bindings.containsKey(symbol)) {
            return new QualifiedNameReference(symbol.toQualifiedName());
        }

        Object value = bindings.get(symbol);

        if (value instanceof String) {
            return Slices.wrappedBuffer(((String) value).getBytes(UTF_8));
        }

        return value;
    }
}
