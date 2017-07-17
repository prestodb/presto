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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ScalarSubqueryToJoinRewriter
{
    private static final QualifiedName COUNT = QualifiedName.of("count");

    private final FunctionRegistry functionRegistry;
    private final SymbolAllocator symbolAllocator;
    private final PlanNodeIdAllocator idAllocator;
    private final Lookup lookup;
    private final PlanNodeDecorrelator planNodeDecorrelator;

    public ScalarSubqueryToJoinRewriter(FunctionRegistry functionRegistry, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "metadata is null");
        this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
        this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
        this.lookup = requireNonNull(lookup, "lookup is null");
        this.planNodeDecorrelator = new PlanNodeDecorrelator(idAllocator, lookup);
    }

    public Optional<PlanNode> rewriteScalarAggregation(LateralJoinNode lateralJoinNode, AggregationNode aggregation)
    {
        List<Symbol> correlation = lateralJoinNode.getCorrelation();
        Optional<DecorrelatedNode> aggregationSource = planNodeDecorrelator.decorrelateFilters(lookup.resolve(aggregation.getSource()), correlation);
        if (!aggregationSource.isPresent()) {
            return Optional.empty();
        }

        return rewriteScalarAggregation(
                lateralJoinNode,
                aggregation,
                aggregationSource.get().getNode(),
                aggregationSource.get().getCorrelatedPredicates());
    }

    private Optional<PlanNode> rewriteScalarAggregation(
            LateralJoinNode lateralJoinNode,
            AggregationNode scalarAggregation,
            PlanNode scalarAggregationSource,
            Optional<Expression> joinExpression)
    {
        SubqueryEquivalentJoin subqueryEquivalentJoin = createSubqueryEquivalentJoin(lateralJoinNode, scalarAggregationSource, joinExpression);

        Optional<AggregationNode> aggregationNode = createAggregationNode(
                scalarAggregation,
                subqueryEquivalentJoin.getJoin(),
                subqueryEquivalentJoin.getNonNull());

        if (!aggregationNode.isPresent()) {
            return Optional.empty();
        }

        Optional<ProjectNode> subqueryProjection = searchFrom(lateralJoinNode.getSubquery(), lookup)
                .where(ProjectNode.class::isInstance)
                .recurseOnlyWhen(EnforceSingleRowNode.class::isInstance)
                .findFirst();

        List<Symbol> aggregationOutputSymbols = getTruncatedAggregationSymbols(lateralJoinNode, aggregationNode.get());

        if (subqueryProjection.isPresent()) {
            Assignments assignments = Assignments.builder()
                    .putIdentities(aggregationOutputSymbols)
                    .putAll(subqueryProjection.get().getAssignments())
                    .build();

            return Optional.of(new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    assignments));
        }
        else {
            return Optional.of(new ProjectNode(
                    idAllocator.getNextId(),
                    aggregationNode.get(),
                    Assignments.identity(aggregationOutputSymbols)));
        }
    }

    private SubqueryEquivalentJoin createSubqueryEquivalentJoin(LateralJoinNode lateralJoinNode, PlanNode decorrelatedSubquery, Optional<Expression> joinExpression)
    {
        AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                idAllocator.getNextId(),
                lateralJoinNode.getInput(),
                symbolAllocator.newSymbol("unique", BigintType.BIGINT));

        Symbol nonNull = symbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
        ProjectNode scalarAggregationSourceWithNonNullableSymbol = new ProjectNode(
                idAllocator.getNextId(),
                decorrelatedSubquery,
                Assignments.builder()
                        .putIdentities(decorrelatedSubquery.getOutputSymbols())
                        .put(nonNull, TRUE_LITERAL)
                        .build());

        JoinNode leftOuterJoin = new JoinNode(
                idAllocator.getNextId(),
                JoinNode.Type.LEFT,
                inputWithUniqueColumns,
                scalarAggregationSourceWithNonNullableSymbol,
                ImmutableList.of(),
                ImmutableList.<Symbol>builder()
                        .addAll(inputWithUniqueColumns.getOutputSymbols())
                        .addAll(scalarAggregationSourceWithNonNullableSymbol.getOutputSymbols())
                        .build(),
                joinExpression,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return new SubqueryEquivalentJoin(leftOuterJoin, nonNull);
    }

    private static List<Symbol> getTruncatedAggregationSymbols(LateralJoinNode lateralJoinNode, AggregationNode aggregationNode)
    {
        Set<Symbol> applySymbols = new HashSet<>(lateralJoinNode.getOutputSymbols());
        return aggregationNode.getOutputSymbols().stream()
                .filter(symbol -> applySymbols.contains(symbol))
                .collect(toImmutableList());
    }

    private Optional<AggregationNode> createAggregationNode(
            AggregationNode scalarAggregation,
            JoinNode leftOuterJoin,
            Symbol nonNullableAggregationSourceSymbol)
    {
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : scalarAggregation.getAggregations().entrySet()) {
            FunctionCall call = entry.getValue().getCall();
            Symbol symbol = entry.getKey();
            if (call.getName().equals(COUNT)) {
                List<TypeSignature> scalarAggregationSourceTypeSignatures = ImmutableList.of(
                        symbolAllocator.getTypes().get(nonNullableAggregationSourceSymbol).getTypeSignature());
                aggregations.put(symbol, new Aggregation(
                        new FunctionCall(
                                COUNT,
                                ImmutableList.of(nonNullableAggregationSourceSymbol.toSymbolReference())),
                        functionRegistry.resolveFunction(
                                COUNT,
                                fromTypeSignatures(scalarAggregationSourceTypeSignatures)),
                        entry.getValue().getMask()));
            }
            else {
                aggregations.put(symbol, entry.getValue());
            }
        }

        List<Symbol> groupBySymbols = leftOuterJoin.getLeft().getOutputSymbols();
        return Optional.of(new AggregationNode(
                idAllocator.getNextId(),
                leftOuterJoin,
                aggregations.build(),
                ImmutableList.of(groupBySymbols),
                scalarAggregation.getStep(),
                scalarAggregation.getHashSymbol(),
                Optional.empty()));
    }

    private static final class SubqueryEquivalentJoin
    {
        private final JoinNode join;
        private final Symbol nonNull;

        private SubqueryEquivalentJoin(JoinNode join, Symbol nonNull)
        {
            this.join = requireNonNull(join, "join is null");
            this.nonNull = requireNonNull(nonNull, "nonNull is null");
        }

        public JoinNode getJoin()
        {
            return join;
        }

        private Symbol getNonNull()
        {
            return nonNull;
        }
    }
}
