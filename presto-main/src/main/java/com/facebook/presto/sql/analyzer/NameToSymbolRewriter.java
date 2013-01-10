package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

public class NameToSymbolRewriter
        extends NodeRewriter<Void>
{
    private final TupleDescriptor descriptor;
    private final Map<Symbol, Type> symbols;

    public NameToSymbolRewriter(TupleDescriptor descriptor)
    {
        this.descriptor = descriptor;
        this.symbols = descriptor.getSymbols();
    }

    @Override
    public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
    {
        List<Field> fields = descriptor.resolve(node.getName());
        if (fields.size() == 1) {
            return new QualifiedNameReference(Iterables.getOnlyElement(fields).getSymbol().toQualifiedName());
        }

        // is this a known symbol?
        if (!node.getName().getPrefix().isPresent() && symbols.containsKey(Symbol.fromQualifiedName(node.getName()))) { // symbols can't have prefixes
            return node;
        }

        throw new IllegalStateException(String.format("%s is a known symbol or field in descriptor %s", descriptor.getFields()));
    }
}

