package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.TreeRewriter;

import java.util.Map;

public class ExpressionNodeInliner
        extends NodeRewriter<Void>
{
    private final Map<? extends Expression, ? extends Expression> mappings;

    public ExpressionNodeInliner(Map<? extends Expression, ? extends Expression> mappings)
    {
        this.mappings = mappings;
    }

    @Override
    public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
    {
        return mappings.get(node);
    }
}
