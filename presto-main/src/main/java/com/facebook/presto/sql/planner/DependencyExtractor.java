package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;

// TODO: a similar class exists in (TupleAnalyzer.DependencyExtractor)
public class DependencyExtractor
{
    public static Set<Symbol> extractUnique(Expression expression)
    {
        return ImmutableSet.copyOf(extractAll(expression));
    }

    public static List<Symbol> extractAll(Expression expression)
    {
        ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
        new Visitor().process(expression, builder);
        return builder.build();
    }

    private static class Visitor
            extends DefaultTraversalVisitor<Void, ImmutableList.Builder<Symbol>>
    {
        @Override
        protected Void visitQualifiedNameReference(QualifiedNameReference node, ImmutableList.Builder<Symbol> builder)
        {
            builder.add(Symbol.fromQualifiedName(node.getName()));
            return null;
        }
    }

}
