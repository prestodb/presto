package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Slot;
import com.facebook.presto.sql.compiler.SlotReference;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

class DependencyExtractor
{
    public Set<Slot> extract(Expression expression)
    {
        ImmutableSet.Builder<Slot> builder = ImmutableSet.builder();

        Visitor visitor = new Visitor();
        visitor.process(expression, builder);

        return builder.build();
    }


    private static class Visitor
            extends DefaultTraversalVisitor<Void, ImmutableSet.Builder<Slot>>
    {
        @Override
        public Void visitSlotReference(SlotReference node, ImmutableSet.Builder<Slot> builder)
        {
            builder.add(node.getSlot());

            return null;
        }
    }

}
