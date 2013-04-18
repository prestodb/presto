package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SymbolAllocator
{
    private final Set<String> blacklist = new HashSet<>();
    private final Map<Symbol, Type> symbols = new HashMap<>();

    public void blacklist(String name)
    {
        blacklist.add(name);
    }

    public Symbol newSymbol(final String name, Type type)
    {
        Preconditions.checkNotNull(name, "name is null");

        String unique = name;

        int id = 1;
        while (symbols.containsKey(new Symbol(unique)) || blacklist.contains(unique)) {
            unique = name + "_"+ id;
            id++;
        }

        Symbol symbol = new Symbol(unique);
        symbols.put(symbol, type);

        return symbol;
    }

    public Symbol newSymbol(Expression expression, Type type)
    {
        if (expression instanceof QualifiedNameReference) {
            QualifiedName name = ((QualifiedNameReference) expression).getName();
            return new Symbol(name.getSuffix());
        }

        return newSymbol("expr", type);
    }

    public Type getType(Symbol symbol)
    {
        return symbols.get(symbol);
    }

    public Map<Symbol, Type> getTypes()
    {
        return ImmutableMap.copyOf(symbols);
    }
}
