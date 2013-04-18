package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class DependencyExtractor
{
    public static Set<Symbol> extract(Expression expression)
    {
        ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();

        Visitor visitor = new Visitor();
        visitor.process(expression, builder);

        return builder.build();
    }


    private static class Visitor
            extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<Symbol>>
    {
        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableSet.Builder<Symbol> builder)
        {
            builder.add(Symbol.fromQualifiedName(node.getName()));
            return null;
        }
    }

}
