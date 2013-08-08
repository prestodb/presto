package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class SymbolToInputRewriter
        extends ExpressionRewriter<Void>
{
    private final Map<Symbol, Input> symbolToInputMapping;

    public SymbolToInputRewriter(Map<Symbol, Input> symbolToInputMapping)
    {
        checkNotNull(symbolToInputMapping, "symbolToInputMapping is null");
        this.symbolToInputMapping = ImmutableMap.copyOf(symbolToInputMapping);
    }

    @Override
    public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        Input input = symbolToInputMapping.get(Symbol.fromQualifiedName(node.getName()));
        Preconditions.checkArgument(input != null, "Cannot resolve symbol %s", node.getName());

        return new InputReference(input);
    }
}
