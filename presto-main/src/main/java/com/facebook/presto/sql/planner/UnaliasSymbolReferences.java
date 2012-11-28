package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.sql.compiler.Symbol;
import com.facebook.presto.sql.compiler.Type;
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
 * Re-maps symbol references that are just aliases of each other (e.g., due to projects like $0 := $1)
 *
 * E.g.,
 *
 * Output[$0, $1] -> Project[$0 := $2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...
 *
 * gets rewritten as
 *
 * Output[$2, $1] -> Project[$2, $1 := $3 * 100] -> Aggregate[$2, $3 := sum($4)] -> ...
 */
public class UnaliasSymbolReferences
        extends PlanOptimizer
{
    @Override
    public PlanNode optimize(PlanNode plan, Map<Symbol, Type> types)
    {
        Visitor visitor = new Visitor(new HashMap<Symbol, Symbol>());
        return plan.accept(visitor, null);
    }

    private static class Visitor
            extends PlanVisitor<Void, PlanNode>
    {
        private final Map<Symbol, Symbol> mapping;

        public Visitor(Map<Symbol, Symbol> mapping)
        {
            this.mapping = mapping;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

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
        public PlanNode visitTableScan(TableScan node, Void context)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                builder.put(canonicalize(entry.getKey()), entry.getValue());
            }

            return new TableScan(node.getTable(), builder.build());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            return new FilterNode(source, canonicalize(node.getPredicate()), canonicalize(node.getOutputSymbols()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

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
        public PlanNode visitOutput(OutputPlan node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

            Map<String, Symbol> canonical = Maps.transformValues(node.getAssignments(), canonicalizeFunction());
            return new OutputPlan(source, node.getColumnNames(), canonical);
        }

        @Override
        public PlanNode visitLimit(LimitNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);
            return new LimitNode(source, node.getCount());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, Void context)
        {
            PlanNode source = node.getSource().accept(this, context);

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
        protected PlanNode visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
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
