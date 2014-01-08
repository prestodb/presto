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

import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.MaterializeSampleNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeRewriter;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

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
        public PlanNode rewriteAggregation(AggregationNode node, Void context, PlanRewriter<Void> planRewriter)
        {
            PlanNode source = planRewriter.rewrite(node.getSource(), null);
            if (source instanceof MaterializeSampleNode) {
                if (node.getFunctions().isEmpty()) {
                    // Aggregation nodes are sometimes used without any functions, as a distinct operator
                    return new AggregationNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), Optional.<Symbol>absent());
                }
                else {
                    return new AggregationNode(node.getId(), ((MaterializeSampleNode) source).getSource(), node.getGroupBy(), node.getAggregations(), node.getFunctions(), node.getMasks(), Optional.of(((MaterializeSampleNode) source).getSampleWeightSymbol()));
                }
            }
            return planRewriter.defaultRewrite(node, null);
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
                    Expression sampleWeightExpr = new ArithmeticExpression(ArithmeticExpression.Type.MULTIPLY, new QualifiedNameReference(leftSampleWeight.toQualifiedName()), new QualifiedNameReference(rightSampleWeight.toQualifiedName()));
                    outputSampleWeight = symbolAllocator.newSymbol(sampleWeightExpr, Type.BIGINT);
                    projections.put(outputSampleWeight, sampleWeightExpr);
                    for (Symbol symbol: Iterables.filter(node.getOutputSymbols(), Predicates.not(Predicates.in(ImmutableSet.of(leftSampleWeight, rightSampleWeight))))) {
                        Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                        projections.put(symbol, expression);
                    }
                    joinNode = new ProjectNode(idAllocator.getNextId(), joinNode, projections.build());
                }
                else {
                    outputSampleWeight = leftSampleWeight == null ? rightSampleWeight : leftSampleWeight;
                }
                return new MaterializeSampleNode(idAllocator.getNextId(), joinNode, outputSampleWeight);

            }
            return planRewriter.defaultRewrite(node, null);
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

            if (!Iterables.any(rewrittenSources, Predicates.instanceOf(MaterializeSampleNode.class))) {
                return planRewriter.defaultRewrite(node, null);
            }

            // Add sample weight to any sources that don't have it, and pull MaterializeSample through the UNION
            ImmutableListMultimap.Builder<Symbol, Symbol> symbolMapping = ImmutableListMultimap.<Symbol, Symbol>builder().putAll(node.getSymbolMapping());
            ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
            Symbol outputSymbol = symbolAllocator.newSymbol(((MaterializeSampleNode) Iterables.getFirst(Iterables.filter(rewrittenSources, Predicates.instanceOf(MaterializeSampleNode.class)), null)).getSampleWeightSymbol().getName(), Type.BIGINT);

            for (PlanNode source: rewrittenSources) {
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
            for (Symbol symbol: source.getOutputSymbols()) {
                Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
                projections.put(symbol, expression);
            }
            Expression one = new LongLiteral("1");
            projections.put(sampleWeightSymbol, one);
            return new ProjectNode(idAllocator.getNextId(), source, projections.build());
        }
    }
}
