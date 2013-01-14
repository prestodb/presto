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
    private final Map<Symbol, Integer> symbolToChannelMapping;

    public SymbolToInputRewriter(Map<Symbol, Integer> symbolToChannelMapping)
    {
        checkNotNull(symbolToChannelMapping, "symbolToChannelMapping is null");
        this.symbolToChannelMapping = symbolToChannelMapping;
    }

    @Override
    public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
    {
        Integer channel = symbolToChannelMapping.get(Symbol.fromQualifiedName(node.getName()));
        Preconditions.checkArgument(channel != null, "Cannot resolve symbol %s", node.getName());

        return new InputReference(new Input(channel, 0)); // TODO: support fields != 0
    }
}
