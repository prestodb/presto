package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.Map;

public class ExpressionSymbolInliner
        extends ExpressionRewriter<Void>
{
    private final Map<Symbol, ? extends Expression> mappings;

    public ExpressionSymbolInliner(Map<Symbol, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
    {
        return mappings.get(Symbol.fromQualifiedName(node.getName()));
    }
}
