package com.facebook.presto.sql.compiler;

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
        // try to resolve name
        List<Field> fields = descriptor.resolve(node.getName());

        if (fields.isEmpty() && symbols.containsKey(Symbol.fromQualifiedName(node.getName()))) {
            // if the name is not a valid field, is it a known symbol?
            return node;
        }

        Symbol symbol = Iterables.getOnlyElement(descriptor.resolve(node.getName())).getSymbol();
        return new QualifiedNameReference(symbol.toQualifiedName());
    }
}

