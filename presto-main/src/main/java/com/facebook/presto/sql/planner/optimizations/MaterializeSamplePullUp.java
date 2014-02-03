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

import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.DeterminismEvaluator;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.base.Predicates.not;

public class MaterializeSamplePullUp
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

        return PlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan, null);
    }

    private static class Rewriter
            extends PlanNodeRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = checkNotNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode rewriteNode(PlanNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            return planRewriter.defaultRewrite(node, null);
        }

        @Override
        public PlanNode rewriteTableWriter(TableWriterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                checkArgument(node.isSampleWeightSupported(), "Cannot write sampled data to a store that doesn't support sampling");
                ConnectorTableMetadata connectorTableMetadata = node.getTableMetadata().getMetadata();
                connectorTableMetadata = new ConnectorTableMetadata(connectorTableMetadata.getTable(), connectorTableMetadata.getColumns(), connectorTableMetadata.getOwner(), true);
                return new TableWriterNode(node.getId(),
                        ((MaterializeSampleNode) source).getSource(),
                        node.getTarget(),
                        node.getColumns(),
                        node.getColumnNames(),
                        node.getOutputSymbols(),
                        Optional.of(((MaterializeSampleNode) source).getSampleWeightSymbol()),
                        node.getCatalog(),
                        new TableMetadata(node.getTableMetadata().getConnectorId(), connectorTableMetadata),
                        node.isSampleWeightSupported());
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteFilter(FilterNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode && DeterminismEvaluator.isDeterministic(node.getPredicate())) {
                FilterNode filterNode = new FilterNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getPredicate());
                return new MaterializeSampleNode(source.getId(), filterNode, ((MaterializeSampleNode) source).getSampleWeightSymbol());
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteProject(ProjectNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode && Iterables.all(node.getExpressions(), DeterminismEvaluator.deterministic())) {
                Symbol sampleWeightSymbol = ((MaterializeSampleNode) source).getSampleWeightSymbol();
                Map<Symbol, Expression> outputMap = ImmutableMap.<Symbol, Expression>builder()
                        .putAll(node.getOutputMap())
                        .put(sampleWeightSymbol, new QualifiedNameReference(sampleWeightSymbol.toQualifiedName()))
                        .build();
                ProjectNode projectNode = new ProjectNode(node.getId(), ((MaterializeSampleNode) source).getSource(), outputMap);
                return new MaterializeSampleNode(source.getId(), projectNode, sampleWeightSymbol);
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteTopN(TopNNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                node = new TopNNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getCount(), node.getOrderBy(), node.getOrderings(), node.isPartial(), Optional.of(((MaterializeSampleNode) source).getSampleWeightSymbol()));
                return new MaterializeSampleNode(source.getId(), node, ((MaterializeSampleNode) source).getSampleWeightSymbol());
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteSort(SortNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                node = new SortNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getOrderBy(), node.getOrderings());
                return new MaterializeSampleNode(source.getId(), node, ((MaterializeSampleNode) source).getSampleWeightSymbol());
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteLimit(LimitNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                node = new LimitNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getCount(), Optional.of(((MaterializeSampleNode) source).getSampleWeightSymbol()));
                return new MaterializeSampleNode(source.getId(), node, ((MaterializeSampleNode) source).getSampleWeightSymbol());
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteDistinctLimit(DistinctLimitNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                // Remove the sample weight, since distinct limit will only output distinct rows
                ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
                for (Symbol symbol : source.getOutputSymbols()) {
                    Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                    projections.put(symbol, expression);
                }
                source = new ProjectNode(idAllocator.getNextId(), ((MaterializeSampleNode) source).getSource(), projections.build());
                return new DistinctLimitNode(node.getId(), source, node.getLimit());
            }
            else {
                return new DistinctLimitNode(node.getId(), source, node.getLimit());
            }
        }

        @Override
        public PlanNode rewriteMarkDistinct(MarkDistinctNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                node = new MarkDistinctNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getMarkerSymbol(), node.getDistinctSymbols(), Optional.of(((MaterializeSampleNode) source).getSampleWeightSymbol()));
                return new MaterializeSampleNode(source.getId(), node, ((MaterializeSampleNode) source).getSampleWeightSymbol());
            }
            else {
                return planRewriter.defaultRewrite(node, null);
            }
        }

        @Override
        public PlanNode rewriteSemiJoin(SemiJoinNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode filteringSource = planRewriter.rewrite(node.getFilteringSource(), null);
            PlanNode source = planRewriter.rewrite(node.getSource(), null);

            if (filteringSource instanceof MaterializeSampleNode) {
                // Materializing the filtering table does nothing, since it will be treated as a set
                filteringSource = ((MaterializeSampleNode) filteringSource).getSource();
            }

            if (source instanceof MaterializeSampleNode) {
                node = new SemiJoinNode(node.getId(), ((MaterializeSampleNode) source).getSource(), filteringSource, node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
                return new MaterializeSampleNode(source.getId(), node, ((MaterializeSampleNode) source).getSampleWeightSymbol());
            }
            else {
                return new SemiJoinNode(node.getId(), source, filteringSource, node.getSourceJoinSymbol(), node.getFilteringSourceJoinSymbol(), node.getSemiJoinOutput());
            }
        }

        @Override
        public PlanNode rewriteAggregation(AggregationNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                if (node.getAggregations().isEmpty() &&
                        node.getOutputSymbols().size() == node.getGroupBy().size() &&
                        node.getOutputSymbols().containsAll(node.getGroupBy())) {
                    // Remove the sample weight, since distinct will only outputs distinct rows
                    ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
                    for (Symbol symbol : source.getOutputSymbols()) {
                        Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                        projections.put(symbol, expression);
                    }
                    source = new ProjectNode(idAllocator.getNextId(), ((MaterializeSampleNode) source).getSource(), projections.build());
                    return new AggregationNode(node.getId(), source, node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), Optional.<Symbol>absent());
                }
                else {
                    return new AggregationNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), Optional.of(((MaterializeSampleNode) source).getSampleWeightSymbol()));
                }
            }

            return new AggregationNode(node.getId(), source, node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), node.getSampleWeight());
        }

        @Override
        public PlanNode rewriteJoin(JoinNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode leftSource = planRewriter.rewrite(node.getLeft(), null);
            PlanNode rightSource = planRewriter.rewrite(node.getRight(), null);

            if (leftSource instanceof MaterializeSampleNode || rightSource instanceof MaterializeSampleNode) {
                Symbol leftSampleWeight = null;
                Symbol rightSampleWeight = null;

                if (leftSource instanceof MaterializeSampleNode) {
                    leftSampleWeight = ((MaterializeSampleNode) leftSource).getSampleWeightSymbol();
                    leftSource = ((MaterializeSampleNode) leftSource).getSource();
                }
                if (rightSource instanceof MaterializeSampleNode) {
                    rightSampleWeight = ((MaterializeSampleNode) rightSource).getSampleWeightSymbol();
                    rightSource = ((MaterializeSampleNode) rightSource).getSource();
                }

                PlanNode joinNode = new JoinNode(node.getId(), node.getType(), leftSource, rightSource, node.getCriteria());
                Symbol outputSampleWeight;
                if (leftSampleWeight != null && rightSampleWeight != null) {
                    ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
                    Expression sampleWeightExpr;
                    switch (node.getType()) {
                        case INNER:
                        case CROSS:
                            sampleWeightExpr = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, new QualifiedNameReference(leftSampleWeight.toQualifiedName()), new QualifiedNameReference(rightSampleWeight.toQualifiedName()));
                            break;
                        case LEFT:
                            sampleWeightExpr = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, new QualifiedNameReference(leftSampleWeight.toQualifiedName()), oneIfNull(rightSampleWeight));
                            break;
                        case RIGHT:
                            sampleWeightExpr = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, oneIfNull(leftSampleWeight), new QualifiedNameReference(rightSampleWeight.toQualifiedName()));
                            break;
                        default:
                            throw new AssertionError(String.format("Unknown join type: %s", node.getType()));
                    }
                    outputSampleWeight = symbolAllocator.newSymbol(sampleWeightExpr, Type.BIGINT);
                    projections.put(outputSampleWeight, sampleWeightExpr);
                    for (Symbol symbol : Iterables.filter(node.getOutputSymbols(), not(in(ImmutableSet.of(leftSampleWeight, rightSampleWeight))))) {
                        Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                        projections.put(symbol, expression);
                    }
                    joinNode = new ProjectNode(idAllocator.getNextId(), joinNode, projections.build());
                }
                else {
                    outputSampleWeight = leftSampleWeight == null ? rightSampleWeight : leftSampleWeight;
                    if ((node.getType() == JoinNode.Type.LEFT && leftSampleWeight == null) || (node.getType() == JoinNode.Type.RIGHT && rightSampleWeight == null)) {
                        // There could be NULLs in the sample weight, so fix them with a projection
                        ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
                        for (Symbol symbol : Iterables.filter(node.getOutputSymbols(), not(equalTo(outputSampleWeight)))) {
                            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                            projections.put(symbol, expression);
                        }
                        Expression sampleWeightExpr = oneIfNull(outputSampleWeight);
                        outputSampleWeight = symbolAllocator.newSymbol(sampleWeightExpr, Type.BIGINT);
                        projections.put(outputSampleWeight, sampleWeightExpr);
                        joinNode = new ProjectNode(idAllocator.getNextId(), joinNode, projections.build());
                    }
                }
                return new MaterializeSampleNode(idAllocator.getNextId(), joinNode, outputSampleWeight);
            }

            return new JoinNode(node.getId(), node.getType(), leftSource, rightSource, node.getCriteria());
        }

        private Expression oneIfNull(Symbol symbol)
        {
            return new CoalesceExpression(new QualifiedNameReference(symbol.toQualifiedName()), new LongLiteral("1"));
        }

        @Override
        public PlanNode rewriteUnion(UnionNode node, Void context, final PlanRewriter<Void> planRewriter)
        {
            List<PlanNode> rewrittenSources = IterableTransformer.on(node.getSources()).transform(new Function<PlanNode, PlanNode>() {
                @Override
                public PlanNode apply(PlanNode input)
                {
                    return planRewriter.rewrite(input, null);
                }
            }).list();

            if (Iterables.all(rewrittenSources, not(instanceOf(MaterializeSampleNode.class)))) {
                return new UnionNode(node.getId(), rewrittenSources, node.getSymbolMapping());
            }

            // Add sample weight to any sources that don't have it, and pull MaterializeSample through the UNION
            ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.<Symbol, Symbol>builder().putAll(node.getSymbolMapping());
            ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
            Symbol outputSymbol = symbolAllocator.newSymbol("$sampleWeight", Type.BIGINT);

            for (PlanNode source : rewrittenSources) {
                if (source instanceof MaterializeSampleNode) {
                    symbolMapping.put(outputSymbol, ((MaterializeSampleNode) source).getSampleWeightSymbol());
                    sources.add(((MaterializeSampleNode) source).getSource());
                }
                else {
                    Symbol symbol = symbolAllocator.newSymbol("$sampleWeight", Type.BIGINT);
                    symbolMapping.put(outputSymbol, symbol);
                    sources.add(addSampleWeight(source, symbol));
                }
            }

            node = new UnionNode(node.getId(), sources.build(), symbolMapping.build());
            return new MaterializeSampleNode(idAllocator.getNextId(), node, outputSymbol);
        }

        private PlanNode addSampleWeight(PlanNode source, Symbol sampleWeightSymbol)
        {
            ImmutableMap.Builder<Symbol, Expression> projections = ImmutableMap.builder();
            for (Symbol symbol : source.getOutputSymbols()) {
                Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                projections.put(symbol, expression);
            }
            Expression one = new LongLiteral("1");
            projections.put(sampleWeightSymbol, one);
            return new ProjectNode(idAllocator.getNextId(), source, projections.build());
        }
    }
}
