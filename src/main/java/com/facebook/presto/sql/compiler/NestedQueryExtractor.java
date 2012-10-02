package com.facebook.presto.sql.compiler;

import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

class NestedQueryExtractor
{
    public List<Query> extract(Expression expression)
    {
        final ImmutableList.Builder<Query> builder = ImmutableList.builder();

        AstVisitor<Void, Void> extractor = new DefaultTraversalVisitor<Void, Void>()
        {
            @Override
            protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
            {
                builder.add(node.getQuery());
                return null;
            }
        };

        extractor.process(expression);
        return builder.build();
    }
}
