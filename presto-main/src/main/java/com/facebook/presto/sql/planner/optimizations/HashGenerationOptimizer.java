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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.DistinctLimitNode;
import com.facebook.presto.sql.planner.plan.IndexJoinNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanRewriter;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.Window;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class HashGenerationOptimizer
        extends PlanOptimizer
{
    public static final int INITIAL_HASH_VALUE = 0;
    private static final String HASH_CODE = FunctionRegistry.mangleOperatorName("HASH_CODE");
    private final boolean optimizeHashGeneration;

    public HashGenerationOptimizer(boolean optimizeHashGeneration)
    {
        this.optimizeHashGeneration = optimizeHashGeneration;
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        checkNotNull(plan, "plan is null");
        checkNotNull(session, "session is null");
        checkNotNull(types, "types is null");
        checkNotNull(symbolAllocator, "symbolAllocator is null");
        checkNotNull(idAllocator, "idAllocator is null");
        if (SystemSessionProperties.isOptimizeHashGenerationEnabled(session, optimizeHashGeneration)) {
            return PlanRewriter.rewriteWith(new Rewriter(idAllocator, symbolAllocator), plan, null);
        }
        return plan;
    }

    private static class Rewriter
            extends PlanRewriter<Void>
    {
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;

        private Rewriter(PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
        {
            this.idAllocator = checkNotNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = checkNotNull(symbolAllocator, "symbolAllocator is null");
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            if (rewrittenSource == node.getSource() && node.getGroupBy().isEmpty()) {
                return node;
            }
            if (node.getGroupBy().isEmpty()) {
                return new AggregationNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getGroupBy(),
                        node.getAggregations(),
                        node.getFunctions(),
                        node.getMasks(),
                        node.getStep(),
                        node.getSampleWeight(),
                        node.getConfidence(),
                        Optional.empty());
            }

            Symbol hashSymbol = symbolAllocator.newHashSymbol();
            PlanNode hashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, hashSymbol, node.getGroupBy());
            return new AggregationNode(idAllocator.getNextId(),
                    hashProjectNode,
                    node.getGroupBy(),
                    node.getAggregations(),
                    node.getFunctions(),
                    node.getMasks(),
                    node.getStep(),
                    node.getSampleWeight(),
                    node.getConfidence(),
                    Optional.of(hashSymbol));
        }

        @Override
        public PlanNode visitDistinctLimit(DistinctLimitNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            Symbol hashSymbol = symbolAllocator.newHashSymbol();
            PlanNode hashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, hashSymbol, node.getOutputSymbols());
            return new DistinctLimitNode(idAllocator.getNextId(), hashProjectNode, node.getLimit(), Optional.of(hashSymbol));
        }

        @Override
        public PlanNode visitMarkDistinct(MarkDistinctNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            Symbol hashSymbol = symbolAllocator.newHashSymbol();
            PlanNode hashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, hashSymbol, node.getDistinctSymbols());
            return new MarkDistinctNode(idAllocator.getNextId(), hashProjectNode, node.getMarkerSymbol(), node.getDistinctSymbols(), Optional.of(hashSymbol));
        }

        @Override
        public PlanNode visitRowNumber(RowNumberNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            if (rewrittenSource == node.getSource() && node.getPartitionBy().isEmpty()) {
                return node;
            }

            if (!node.getPartitionBy().isEmpty()) {
                Symbol hashSymbol = symbolAllocator.newHashSymbol();
                PlanNode hashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, hashSymbol, node.getPartitionBy());
                return new RowNumberNode(idAllocator.getNextId(), hashProjectNode, node.getPartitionBy(), node.getRowNumberSymbol(), node.getMaxRowCountPerPartition(), Optional.of(hashSymbol));
            }
            return new RowNumberNode(idAllocator.getNextId(), rewrittenSource, node.getPartitionBy(), node.getRowNumberSymbol(), node.getMaxRowCountPerPartition(), node.getHashSymbol());
        }

        @Override
        public PlanNode visitTopNRowNumber(TopNRowNumberNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            if (rewrittenSource == node.getSource() && node.getPartitionBy().isEmpty()) {
                return node;
            }

            if (node.getPartitionBy().isEmpty()) {
                return new TopNRowNumberNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        node.getOrderBy(),
                        node.getOrderings(),
                        node.getRowNumberSymbol(),
                        node.getMaxRowCountPerPartition(),
                        node.isPartial(),
                        node.getHashSymbol());
            }
            Symbol hashSymbol = symbolAllocator.newHashSymbol();
            PlanNode hashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, hashSymbol, node.getPartitionBy());
            return new TopNRowNumberNode(idAllocator.getNextId(),
                    hashProjectNode,
                    node.getPartitionBy(),
                    node.getOrderBy(),
                    node.getOrderings(),
                    node.getRowNumberSymbol(),
                    node.getMaxRowCountPerPartition(),
                    node.isPartial(),
                    Optional.of(hashSymbol));
        }

        @Override
        public PlanNode visitJoin(JoinNode node, RewriteContext<Void> context)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> leftSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getLeft);
            List<Symbol> rightSymbols = Lists.transform(clauses, JoinNode.EquiJoinClause::getRight);

            PlanNode rewrittenLeft = context.rewrite(node.getLeft(), null);
            PlanNode rewrittenRight = context.rewrite(node.getRight(), null);

            Symbol leftHashSymbol = symbolAllocator.newHashSymbol();
            Symbol rightHashSymbol = symbolAllocator.newHashSymbol();

            PlanNode leftHashProjectNode = getHashProjectNode(idAllocator, rewrittenLeft, leftHashSymbol, leftSymbols);
            PlanNode rightHashProjectNode = getHashProjectNode(idAllocator, rewrittenRight, rightHashSymbol, rightSymbols);

            return new JoinNode(idAllocator.getNextId(), node.getType(), leftHashProjectNode, rightHashProjectNode, node.getCriteria(), Optional.of(leftHashSymbol), Optional.of(rightHashSymbol));
        }

        @Override
        public PlanNode visitSemiJoin(SemiJoinNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            PlanNode rewrittenFilteringSource = context.rewrite(node.getFilteringSource(), null);

            Symbol sourceHashSymbol = symbolAllocator.newHashSymbol();
            Symbol filteringSourceHashSymbol = symbolAllocator.newHashSymbol();

            PlanNode sourceHashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, sourceHashSymbol, ImmutableList.of(node.getSourceJoinSymbol()));
            PlanNode filteringSourceHashProjectNode = getHashProjectNode(idAllocator, rewrittenFilteringSource, filteringSourceHashSymbol, ImmutableList.of(node.getFilteringSourceJoinSymbol()));

            return new SemiJoinNode(idAllocator.getNextId(),
                    sourceHashProjectNode,
                    filteringSourceHashProjectNode,
                    node.getSourceJoinSymbol(),
                    node.getFilteringSourceJoinSymbol(),
                    node.getSemiJoinOutput(),
                    Optional.of(sourceHashSymbol),
                    Optional.of(filteringSourceHashSymbol));
        }

        @Override
        public PlanNode visitIndexJoin(IndexJoinNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenIndex = context.rewrite(node.getIndexSource(), null);
            PlanNode rewrittenProbe = context.rewrite(node.getProbeSource(), null);

            Symbol indexHashSymbol = symbolAllocator.newHashSymbol();
            Symbol probeHashSymbol = symbolAllocator.newHashSymbol();

            List<IndexJoinNode.EquiJoinClause> clauses = node.getCriteria();

            List<Symbol> indexSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getIndex);
            List<Symbol> probeSymbols = Lists.transform(clauses, IndexJoinNode.EquiJoinClause::getProbe);

            PlanNode indexHashProjectNode = getHashProjectNode(idAllocator, rewrittenIndex, indexHashSymbol, indexSymbols);
            PlanNode probeHashProjectNode = getHashProjectNode(idAllocator, rewrittenProbe, probeHashSymbol, probeSymbols);

            return new IndexJoinNode(idAllocator.getNextId(),
                    node.getType(),
                    probeHashProjectNode,
                    indexHashProjectNode,
                    node.getCriteria(),
                    Optional.of(probeHashSymbol),
                    Optional.of(indexHashSymbol));
        }

        @Override
        public PlanNode visitWindow(WindowNode node, RewriteContext<Void> context)
        {
            PlanNode rewrittenSource = context.rewrite(node.getSource(), null);
            if (rewrittenSource == node.getSource() && node.getPartitionBy().isEmpty()) {
                return node;
            }
            if (node.getPartitionBy().isEmpty()) {
                return new WindowNode(idAllocator.getNextId(),
                        rewrittenSource,
                        node.getPartitionBy(),
                        node.getOrderBy(),
                        node.getOrderings(),
                        node.getFrame(),
                        node.getWindowFunctions(),
                        node.getSignatures(),
                        Optional.empty(),
                        node.getPrePartitionedInputs(),
                        node.getPreSortedOrderPrefix());
            }
            Symbol hashSymbol = symbolAllocator.newHashSymbol();
            PlanNode hashProjectNode = getHashProjectNode(idAllocator, rewrittenSource, hashSymbol, node.getPartitionBy());
            return new WindowNode(idAllocator.getNextId(),
                    hashProjectNode,
                    node.getPartitionBy(),
                    node.getOrderBy(),
                    node.getOrderings(),
                    node.getFrame(),
                    node.getWindowFunctions(),
                    node.getSignatures(),
                    Optional.of(hashSymbol),
                    node.getPrePartitionedInputs(),
                    node.getPreSortedOrderPrefix());
        }
    }

    private static ProjectNode getHashProjectNode(PlanNodeIdAllocator idAllocator, PlanNode source, Symbol hashSymbol, List<Symbol> partitioningSymbols)
    {
        checkArgument(!partitioningSymbols.isEmpty(), "partitioningSymbols is empty");
        ImmutableMap.Builder<Symbol, Expression> outputSymbols = ImmutableMap.builder();
        for (Symbol symbol : source.getOutputSymbols()) {
            Expression expression = new QualifiedNameReference(symbol.toQualifiedName());
            outputSymbols.put(symbol, expression);
        }

        Expression hashExpression = getHashExpression(partitioningSymbols);
        outputSymbols.put(hashSymbol, hashExpression);
        return new ProjectNode(idAllocator.getNextId(), source, outputSymbols.build());
    }

    private static Expression getHashExpression(List<Symbol> partitioningSymbols)
    {
        Expression hashExpression = new LongLiteral(String.valueOf(INITIAL_HASH_VALUE));
        for (Symbol symbol : partitioningSymbols) {
            hashExpression = getHashFunctionCall(hashExpression, symbol);
        }
        return hashExpression;
    }

    private static Expression getHashFunctionCall(Expression previousHashValue, Symbol symbol)
    {
        FunctionCall functionCall = new FunctionCall(QualifiedName.of(HASH_CODE), Optional.<Window>empty(), false, ImmutableList.<Expression>of(new QualifiedNameReference(symbol.toQualifiedName())));
        List<Expression> arguments = ImmutableList.of(previousHashValue, orNullHashCode(functionCall));
        return new FunctionCall(QualifiedName.of("combine_hash"), arguments);
    }

    private static Expression orNullHashCode(Expression expression)
    {
        return new CoalesceExpression(expression, new LongLiteral(String.valueOf(TypeUtils.NULL_HASH_CODE)));
    }
}
