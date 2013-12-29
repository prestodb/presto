/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MaterializedViewWriterNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");

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
        public PlanNode rewriteMaterializedViewWriter(MaterializedViewWriterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            ImmutableMap.Builder<Symbol, ColumnHandle> columns = ImmutableMap.builder();

            for (Map.Entry<Symbol, ColumnHandle> entry : node.getColumns().entrySet()) {
                columns.put(canonicalize(entry.getKey()), entry.getValue());
            }

            return new MaterializedViewWriterNode(node.getId(), source, node.getTable(), columns.build(), canonicalize(node.getOutput()));
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

            ImmutableList<Symbol> groupByKeys = ImmutableList.copyOf(ImmutableSet.copyOf(canonicalize(node.getGroupBy())));
            return new AggregationNode(node.getId(), source, groupByKeys, functionCalls.build(), functionInfos.build());
        }

        @Override
        public PlanNode rewriteWindow(WindowNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            ImmutableMap.Builder<Symbol, FunctionHandle> functionInfos = ImmutableMap.builder();
            ImmutableMap.Builder<Symbol, FunctionCall> functionCalls = ImmutableMap.builder();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();
                Symbol canonical = canonicalize(symbol);
                functionCalls.put(canonical, (FunctionCall) canonicalize(entry.getValue()));
                functionInfos.put(canonical, node.getFunctionHandles().get(symbol));
            }

            ImmutableMap.Builder<Symbol, SortItem.Ordering> orderings = ImmutableMap.builder();
            for (Map.Entry<Symbol, SortItem.Ordering> entry : node.getOrderings().entrySet()) {
                orderings.put(canonicalize(entry.getKey()), entry.getValue());
            }

            return new WindowNode(node.getId(), source, canonicalize(node.getPartitionBy()), canonicalize(node.getOrderBy()), orderings.build(), functionCalls.build(), functionInfos.build());
        }

        @Override
        public PlanNode rewriteTableScan(TableScanNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            ImmutableMap.Builder<Symbol, ColumnHandle> builder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getAssignments().entrySet()) {
                builder.put(canonicalize(entry.getKey()), entry.getValue());
            }

            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = canonicalize(node.getOriginalConstraint());
            }
            return new TableScanNode(node.getId(), node.getTable(), canonicalize(node.getOutputSymbols()), builder.build(), originalConstraint, node.getGeneratedPartitions());
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            return new FilterNode(node.getId(), source, canonicalize(node.getPredicate()));
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            Map<Symbol, Expression> assignments = new LinkedHashMap<>();
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

            return new ProjectNode(node.getId(), source, assignments);
        }

        @Override
        public PlanNode rewriteOutput(OutputNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            List<Symbol> canonical = Lists.transform(node.getOutputSymbols(), canonicalizeFunction());
            return new OutputNode(node.getId(), source, node.getColumnNames(), canonical);
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

            return new TopNNode(node.getId(), source, node.getCount(), symbols.build(), orderings.build(), node.isPartial());
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

            return new SortNode(node.getId(), source, symbols.build(), orderings.build());
        }

        @Override
        public PlanNode rewriteJoin(JoinNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode left = planRewriter.rewrite(node.getLeft(), context);
            PlanNode right = planRewriter.rewrite(node.getRight(), context);

            return new JoinNode(node.getId(), node.getType(), left, right, canonicalizeJoinCriteria(node.getCriteria()));
        }

        @Override
        public PlanNode rewriteSemiJoin(SemiJoinNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);
            PlanNode filteringSource = planRewriter.rewrite(node.getFilteringSource(), context);

            return new SemiJoinNode(node.getId(), source, filteringSource, canonicalize(node.getSourceJoinSymbol()), canonicalize(node.getFilteringSourceJoinSymbol()), canonicalize(node.getSemiJoinOutput()));
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            ImmutableList.Builder<PlanNode> rewrittenSources = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                rewrittenSources.add(planRewriter.rewrite(source, context));
            }

            return new UnionNode(node.getId(), rewrittenSources.build(), canonicalizeUnionSymbolMap(node.getSymbolMapping()));
        }

        @Override
        public PlanNode rewriteTableWriter(TableWriterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), context);

            return new TableWriterNode(node.getId(), source, node.getTarget(), canonicalize(node.getColumns()), node.getColumnNames(), canonicalize(node.getOutputSymbols()));
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
            return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
            {
                @Override
                public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
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

        private List<JoinNode.EquiJoinClause> canonicalizeJoinCriteria(List<JoinNode.EquiJoinClause> criteria)
        {
            ImmutableList.Builder<JoinNode.EquiJoinClause> builder = ImmutableList.builder();
            for (JoinNode.EquiJoinClause clause : criteria) {
                builder.add(new JoinNode.EquiJoinClause(canonicalize(clause.getLeft()), canonicalize(clause.getRight())));
            }

            return builder.build();
        }

        private ListMultimap<Symbol, Symbol> canonicalizeUnionSymbolMap(ListMultimap<Symbol, Symbol> unionSymbolMap)
        {
            ImmutableListMultimap.Builder<Symbol, Symbol> builder = ImmutableListMultimap.builder();
            for (Map.Entry<Symbol, Collection<Symbol>> entry : unionSymbolMap.asMap().entrySet()) {
                builder.putAll(canonicalize(entry.getKey()), Iterables.transform(entry.getValue(), canonicalizeFunction()));
            }
            return builder.build();
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
