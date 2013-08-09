package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Extracts and returns the set of all expression subtrees within an Expression, including Expression itself
 */
public class SubExpressionExtractor
{
    public static Set<Expression> extract(Expression expression)
    {
        ImmutableSet.Builder<Expression> builder = ImmutableSet.builder();
        // Borrow the TreeRewriter as a way to walk the Expression tree with a visitor that still respects the type inheritance
        // We actually don't care about the rewrite result, just the context that gets updated through the walk
        TreeRewriter.rewriteWith(new Visitor(), expression, builder);
        return builder.build();
    }

    private static class Visitor
            extends NodeRewriter<ImmutableSet.Builder<Expression>>
    {
        @Override
        public Node rewriteExpression(Expression node, ImmutableSet.Builder<Expression> builder, TreeRewriter<ImmutableSet.Builder<Expression>> treeRewriter)
        {
            builder.add(node);
            return null;
        }
    }
}
