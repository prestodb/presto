package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.compiler.Symbol;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Computes all symbols declared by a logical plan
 */
public class SymbolExtractor
{
    public static Set<Symbol> extract(PlanNode node)
    {
        ImmutableSet.Builder<Symbol> builder = ImmutableSet.builder();

        node.accept(new Visitor(builder), null);

        return builder.build();
    }

    private static class Visitor
            extends PlanVisitor<Void, Void>
    {
        private final ImmutableSet.Builder<Symbol> builder;

        public Visitor(ImmutableSet.Builder<Symbol> builder)
        {
            this.builder = builder;
        }

        @Override
        public Void visitExchange(ExchangeNode node, Void context)
        {
            builder.addAll(node.getOutputSymbols());

            return null;
        }

        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            // visit child
            node.getSource().accept(this, context);

            builder.addAll(node.getAggregations().keySet());

            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Void context)
        {
            // visit child
            node.getSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitProject(ProjectNode node, Void context)
        {
            // visit child
            node.getSource().accept(this, context);

            builder.addAll(node.getOutputSymbols());

            return null;
        }

        @Override
        public Void visitTopN(TopNNode node, Void context)
        {
            node.getSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitOutput(OutputPlan node, Void context)
        {
            node.getSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitLimit(LimitNode node, Void context)
        {
            node.getSource().accept(this, context);

            return null;
        }

        @Override
        public Void visitTableScan(TableScan node, Void context)
        {
            builder.addAll(node.getAssignments().keySet());

            return null;
        }

        @Override
        public Void visitJoin(JoinNode node, Void context)
        {
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            return null;
        }

        @Override
        protected Void visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }
    }
}
