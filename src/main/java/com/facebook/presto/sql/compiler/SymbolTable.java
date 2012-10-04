package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.compiler.Field.nameGetter;
import static com.facebook.presto.sql.tree.QualifiedName.hasSuffixPredicate;
import static com.google.common.base.Predicates.compose;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;

public class SymbolTable
{
    private final SymbolTable parent;
    private final Set<Field> fields;

    public SymbolTable()
    {
        parent = null;
        fields = ImmutableSet.of();
    }

    public SymbolTable(Iterable<Field> fields)
    {
        parent = null;
        this.fields = ImmutableSet.copyOf(fields);
    }

    public SymbolTable(SymbolTable parent, Iterable<Field> fields)
    {
        Preconditions.checkNotNull(parent, "parent is null");

        this.parent = parent;
        this.fields = ImmutableSet.copyOf(fields);
    }

    public List<QualifiedName> resolve(QualifiedName suffix)
    {
        List<QualifiedName> matches = ImmutableList.copyOf(filter(transform(fields, nameGetter()), hasSuffixPredicate(suffix)));
        if (matches.isEmpty() && parent != null) {
            return parent.resolve(suffix);
        }
        return matches;
    }
}
