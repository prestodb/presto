package com.facebook.presto.sql.planner;

import com.facebook.presto.operator.Input;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Preconditions;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SymbolToInputRewriter
        extends NodeRewriter<Void>
{
    private final Map<Symbol, Input> symbolToInputMapping;

    public SymbolToInputRewriter(Map<Symbol, Input> symbolToInputMapping)
    {
        checkNotNull(symbolToInputMapping, "symbolToInputMapping is null");
        this.symbolToInputMapping = symbolToInputMapping;
    }

    @Override
    public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
    {
        Input input = symbolToInputMapping.get(Symbol.fromQualifiedName(node.getName()));
        Preconditions.checkArgument(input != null, "Cannot resolve symbol %s", node.getName());

        return new InputReference(input);
    }
}
