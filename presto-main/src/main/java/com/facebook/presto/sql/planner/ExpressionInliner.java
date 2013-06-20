package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Preconditions;

import java.util.Map;

public class ExpressionInliner
        extends NodeRewriter<Void>
{
    private final Map<Symbol, ? extends Expression> mappings;

    public ExpressionInliner(Map<Symbol, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
    {
        return mappings.get(Symbol.fromQualifiedName(node.getName()));
    }
}
