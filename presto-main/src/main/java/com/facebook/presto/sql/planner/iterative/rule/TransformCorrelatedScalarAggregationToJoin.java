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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.plan.AggregationNode;
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

import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypeSignatures;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.LateralJoin.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Scalar aggregation is aggregation with GROUP BY 'a constant' (or empty GROUP BY).
 * It always returns single row.
 * <p>
 * This optimizer rewrites correlated scalar aggregation subquery to left outer join in a way described here:
 * https://github.com/prestodb/presto/wiki/Correlated-subqueries
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Aggregation(GROUP BY (); functions: [sum(F), count(), ...]
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Aggregation(GROUP BY A, B, C, U; functions: [sum(F), count(non_null), ...]
 *   - Join(LEFT_OUTER, D = C)
 *     - AssignUniqueId(adds symbol U)
 *       - (input) plan which produces symbols: [A, B, C]
 *     - Filter(E > 5)
 *       - projection which adds non null symbol used for count() function
 *         - plan which produces symbols: [D, E, F]
 * </pre>
 * <p>
 * Note that only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedScalarAggregationToJoin
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    private final FunctionRegistry functionRegistry;

    public TransformCorrelatedScalarAggregationToJoin(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode subquery = lateralJoinNode.getSubquery();

        if (!isScalar(subquery, context.getLookup())) {
            return Result.empty();
        }

        Optional<AggregationNode> aggregation = findAggregation(subquery, context.getLookup());
        if (!(aggregation.isPresent() && aggregation.get().getGroupingKeys().isEmpty())) {
            return Result.empty();
        }

        Rewriter rewriter = new Rewriter(functionRegistry, context.getSymbolAllocator(), context.getIdAllocator(), context.getLookup());

        PlanNode rewrittenNode = rewriter.rewriteScalarAggregation(lateralJoinNode, aggregation.get());

        if (rewrittenNode instanceof LateralJoinNode) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewrittenNode);
    }

    private static Optional<AggregationNode> findAggregation(PlanNode rootNode, Lookup lookup)
    {
        return searchFrom(rootNode, lookup)
                .where(AggregationNode.class::isInstance)
                .recurseOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                .findFirst();
    }

    private static class Rewriter
    {
        private static final QualifiedName COUNT = QualifiedName.of("count");

        private final FunctionRegistry functionRegistry;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Lookup lookup;
        private final PlanNodeDecorrelator planNodeDecorrelator;

        public Rewriter(FunctionRegistry functionRegistry, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
        {
            this.functionRegistry = requireNonNull(functionRegistry, "metadata is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.planNodeDecorrelator = new PlanNodeDecorrelator(idAllocator, lookup);
        }

        public PlanNode rewriteScalarAggregation(LateralJoinNode lateralJoinNode, AggregationNode aggregation)
        {
            List<Symbol> correlation = lateralJoinNode.getCorrelation();
            Optional<PlanNodeDecorrelator.DecorrelatedNode> source = planNodeDecorrelator.decorrelateFilters(lookup.resolve(aggregation.getSource()), correlation);
            if (!source.isPresent()) {
                return lateralJoinNode;
            }

            Symbol nonNull = symbolAllocator.newSymbol("non_null", BooleanType.BOOLEAN);
            Assignments scalarAggregationSourceAssignments = Assignments.builder()
                    .putIdentities(source.get().getNode().getOutputSymbols())
                    .put(nonNull, TRUE_LITERAL)
                    .build();
            ProjectNode scalarAggregationSourceWithNonNullableSymbol = new ProjectNode(
                    idAllocator.getNextId(),
                    source.get().getNode(),
                    scalarAggregationSourceAssignments);

            return rewriteScalarAggregation(
                    lateralJoinNode,
                    aggregation,
                    scalarAggregationSourceWithNonNullableSymbol,
                    source.get().getCorrelatedPredicates(),
                    nonNull);
        }

        private PlanNode rewriteScalarAggregation(
                LateralJoinNode lateralJoinNode,
                AggregationNode scalarAggregation,
                PlanNode scalarAggregationSource,
                Optional<Expression> joinExpression,
                Symbol nonNull)
        {
            AssignUniqueId inputWithUniqueColumns = new AssignUniqueId(
                    idAllocator.getNextId(),
                    lateralJoinNode.getInput(),
                    symbolAllocator.newSymbol("unique", BigintType.BIGINT));

            JoinNode leftOuterJoin = new JoinNode(
                    idAllocator.getNextId(),
                    JoinNode.Type.LEFT,
                    inputWithUniqueColumns,
                    scalarAggregationSource,
                    ImmutableList.of(),
                    ImmutableList.<Symbol>builder()
                            .addAll(inputWithUniqueColumns.getOutputSymbols())
                            .addAll(scalarAggregationSource.getOutputSymbols())
                            .build(),
                    joinExpression,
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty());

            Optional<AggregationNode> aggregationNode = createAggregationNode(
                    scalarAggregation,
                    leftOuterJoin,
                    nonNull);

            if (!aggregationNode.isPresent()) {
                return lateralJoinNode;
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

                return new ProjectNode(
                        idAllocator.getNextId(),
                        aggregationNode.get(),
                        assignments);
            }
            else {
                return new ProjectNode(
                        idAllocator.getNextId(),
                        aggregationNode.get(),
                        Assignments.identity(aggregationOutputSymbols));
            }
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
            ImmutableMap.Builder<Symbol, AggregationNode.Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> entry : scalarAggregation.getAggregations().entrySet()) {
                FunctionCall call = entry.getValue().getCall();
                Symbol symbol = entry.getKey();
                if (call.getName().equals(COUNT)) {
                    List<TypeSignature> scalarAggregationSourceTypeSignatures = ImmutableList.of(
                            symbolAllocator.getTypes().get(nonNullableAggregationSourceSymbol).getTypeSignature());
                    aggregations.put(symbol, new AggregationNode.Aggregation(
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
    }
}
