package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;

public class SymbolTable
{
    private final SymbolTable parent;
    private final Set<QualifiedName> names;

    public SymbolTable()
    {
        parent = null;
        names = ImmutableSet.of();
    }

    public SymbolTable(Iterable<QualifiedName> names)
    {
        parent = null;
        this.names = ImmutableSet.copyOf(names);
    }

    public SymbolTable(SymbolTable parent, Iterable<QualifiedName> names)
    {
        Preconditions.checkNotNull(parent, "parent is null");

        this.parent = parent;
        this.names = ImmutableSet.copyOf(names);
    }

    public List<QualifiedName> resolve(QualifiedName suffix)
    {
        List<QualifiedName> matches = ImmutableList.copyOf(Iterables.filter(names, QualifiedName.hasSuffix(suffix)));
        if (matches.isEmpty() && parent != null) {
            return parent.resolve(suffix);
        }
        return matches;
    }
}
