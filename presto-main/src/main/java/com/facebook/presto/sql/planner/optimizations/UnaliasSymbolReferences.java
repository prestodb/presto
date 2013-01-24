package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Re-maps symbol references that are just aliases of each other (e.g., due to projections like {@code $0 := $1})
 * <p/>
 * E.g.,
 * <p/>
 * {@code Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 * <p/>
 * gets rewritten as
 * <p/>
 * {@code Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...}
 */
public class UnaliasSymbolReferences
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
        return PlanRewriter.rewriteWith(new Rewriter(new HashMap<Symbol, Symbol>()), plan);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        private final Map<Symbol, Symbol> mapping;

        public Rewriter(Map<Symbol, Symbol> mapping)
        {
            this.mapping = mapping;
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            ImmutableMap.Builder<Symbol, FunctionHandle> functionInfos = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Symbol canonical = canonicalize(symbol);
                functionCalls.put(canonical, (FunctionCall) canonicalize(entry.getValue()));
                functionInfos.put(canonical, node.getFunctions().get(symbol));
            }

            return new AggregationNode(source, canonicalize(node.getGroupBy()), functionCalls.build(), functionInfos.build());
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                builder.put(canonicalize(entry.getKey()), entry.getValue());
            }

            return new TableScanNode(node.getTable(), builder.build());
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            return new FilterNode(source, canonicalize(node.getPredicate()), canonicalize(node.getOutputSymbols()));
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            Map<Symbol, Expression> assignments = new HashMap<>();
            for (Map.Entry<Symbol, Expression> entry : node.getOutputMap().entrySet()) {
                Expression expression = canonicalize(entry.getValue());

                if (entry.getValue() instanceof QualifiedNameReference) {
                    Symbol symbol = Symbol.fromQualifiedName(((QualifiedNameReference) entry.getValue()).getName());
                    if (!symbol.equals(entry.getKey())) {
                        map(entry.getKey(), symbol);
                    }
                }

                Symbol canonical = canonicalize(entry.getKey());

                if (!assignments.containsKey(canonical)) {
                    assignments.put(canonical, expression);
                }
            }

            return new ProjectNode(source, assignments);
        }

        @Override
        public PlanNode rewriteOutput(OutputNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            Map<String, Symbol> canonical = Maps.transformValues(node.getAssignments(), canonicalizeFunction());
            return new OutputNode(source, node.getColumnNames(), canonical);
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortItem.Ordering> orderings = ImmutableMap.builder();
            for (Symbol symbol : node.getOrderBy()) {
                Symbol canonical = canonicalize(symbol);
                symbols.add(canonical);
                orderings.put(canonical, node.getOrderings().get(symbol));
            }

            return new TopNNode(source, node.getCount(), symbols.build(), orderings.build());
        }

        @Override
        public PlanNode rewriteSort(SortNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortItem.Ordering> orderings = ImmutableMap.builder();
            for (Symbol symbol : node.getOrderBy()) {
                Symbol canonical = canonicalize(symbol);
                symbols.add(canonical);
                orderings.put(canonical, node.getOrderings().get(symbol));
            }

            return new SortNode(source, symbols.build(), orderings.build());
        }

        @Override
        public PlanNode rewriteJoin(JoinNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode left = planRewriter.rewrite(node.getLeft(), context);
            PlanNode right = planRewriter.rewrite(node.getRight(), context);

            return new JoinNode(left, right, canonicalize(node.getCriteria()));
        }

        private void map(Symbol symbol, Symbol canonical)
        {
            Preconditions.checkArgument(!symbol.equals(canonical), "Can't map symbol to itself: %s", symbol);
            mapping.put(symbol, canonical);
        }

        private Symbol canonicalize(Symbol symbol)
        {
            Symbol canonical = symbol;
            while (mapping.containsKey(canonical)) {
                canonical = mapping.get(canonical);
            }
            return canonical;
        }

        private Expression canonicalize(Expression value)
        {
            return TreeRewriter.rewriteWith(new NodeRewriter<Void>()
            {
                @Override
                public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
                {
                    Symbol canonical = canonicalize(Symbol.fromQualifiedName(node.getName()));
                    return new QualifiedNameReference(canonical.toQualifiedName());
                }
            }, value);
        }

        private List<Symbol> canonicalize(List<Symbol> outputs)
        {
            return Lists.transform(outputs, canonicalizeFunction());
        }

        private Function<Symbol, Symbol> canonicalizeFunction()
        {
            return new Function<Symbol, Symbol>()
            {
                @Override
                public Symbol apply(Symbol input)
                {
                    return canonicalize(input);
                }
            };
        }
    }
}
