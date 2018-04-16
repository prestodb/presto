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
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.optimizations.ScalarSubqueryToJoinRewriter;
import com.facebook.presto.sql.planner.optimizations.ScalarSubqueryToJoinRewriter.SubqueryEquivalentJoin;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.planner.plan.MarkDistinctNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.spi.StandardErrorCode.SUBQUERY_MULTIPLE_ROWS;
import static com.facebook.presto.spi.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.LateralJoin.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static java.util.Objects.requireNonNull;

/**
 * Scalar filter scan query is something like:
 * <pre>
 *     SELECT a,b,c FROM rel WHERE a = correlated1 AND b = correlated2
 * </pre>
 * <p>
 * This optimizer can rewrite to aggregation over a left outer join:
 * <p>
 * From:
 * <pre>
 * - LateralJoin (with correlation list: [C])
 *   - (input) plan which produces symbols: [A, B, C]
 *   - (subquery) Project F
 *     - Filter(D = C AND E > 5)
 *       - plan which produces symbols: [D, E, F]
 * </pre>
 * to:
 * <pre>
 * - Filter(CASE isDistinct WHEN true THEN true ELSE fail('Scalar sub-query has returned multiple rows'))
 *   - MarkDistinct(isDistinct)
 *     - Join(LEFT_OUTER, D = C)
 *       - AssignUniqueId(adds symbol U)
 *         - (input) plan which produces symbols: [A, B, C]
 *       - Filter(E > 5)
 *         - projection which adds no null symbol used for count() function
 *           - plan which produces symbols: [D, E, F1]
 * </pre>
 * <p>
 * Note only conjunction predicates in FilterNode are supported
 */
public class TransformCorrelatedNonAggregationScalarToJoin
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    private final FunctionRegistry functionRegistry;

    public TransformCorrelatedNonAggregationScalarToJoin(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode subquery = context.getLookup().resolve(lateralJoinNode.getSubquery());

        if (!isScalar(subquery, context.getLookup())) {
            return Result.empty();
        }

        Optional<FilterNode> filterScan = getFilterScan(subquery, context.getLookup());
        if (!filterScan.isPresent()) {
            return Result.empty();
        }

        Optional<PlanNode> rewritten = rewriteScalarFilterScan(lateralJoinNode, filterScan.get(), context);
        if (!rewritten.isPresent()) {
            return Result.empty();
        }

        return Result.ofPlanNode(rewritten.get());
    }

    private Optional<FilterNode> getFilterScan(PlanNode planNode, Lookup lookup)
    {
        return searchFrom(planNode, lookup)
                .where(FilterNode.class::isInstance)
                .recurseOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                .findFirst();
    }

    private Optional<PlanNode> rewriteScalarFilterScan(LateralJoinNode lateralJoinNode, FilterNode filterNode, Context context)
    {
        PlanNodeDecorrelator planNodeDecorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getLookup());
        ScalarSubqueryToJoinRewriter rewriter = new ScalarSubqueryToJoinRewriter(functionRegistry, context.getSymbolAllocator(), context.getIdAllocator(), context.getLookup());
        List<Symbol> correlation = lateralJoinNode.getCorrelation();
        Optional<DecorrelatedNode> decorrelatedSubquerySource = planNodeDecorrelator.decorrelateFilters(filterNode, correlation);
        if (!decorrelatedSubquerySource.isPresent()) {
            return Optional.empty();
        }

        // we need to map output symbols to some different symbols, so output symbols could be used as output of aggregation
        PlanNode decorrelatedSubquerySourceNode = decorrelatedSubquerySource.get().getNode();

        SubqueryEquivalentJoin subqueryEquivalentJoin = rewriter.createSubqueryEquivalentJoin(
                lateralJoinNode,
                decorrelatedSubquerySourceNode,
                decorrelatedSubquerySource.get().getCorrelatedPredicates());

        Symbol isDistinct = context.getSymbolAllocator().newSymbol("is_distinct", BooleanType.BOOLEAN);
        MarkDistinctNode markDistinctNode = new MarkDistinctNode(
                context.getIdAllocator().getNextId(),
                subqueryEquivalentJoin.getJoin(),
                isDistinct,
                subqueryEquivalentJoin.getInput().getOutputSymbols(),
                Optional.empty());

        FilterNode filterNodeToMakeSureThatSubqueryIsScalar = new FilterNode(
                context.getIdAllocator().getNextId(),
                markDistinctNode,
                new SimpleCaseExpression(
                        isDistinct.toSymbolReference(),
                        ImmutableList.of(
                                new WhenClause(TRUE_LITERAL, TRUE_LITERAL)),
                        Optional.of(new Cast(
                                new FunctionCall(
                                        QualifiedName.of("fail"),
                                        ImmutableList.of(
                                                new LongLiteral(Integer.toString(SUBQUERY_MULTIPLE_ROWS.toErrorCode().getCode())),
                                                new StringLiteral("Scalar sub-query has returned multiple rows"))),
                                BOOLEAN))));

        return rewriter.projectToLateralOutputSymbols(
                lateralJoinNode,
                filterNodeToMakeSureThatSubqueryIsScalar,
                Optional.of(subqueryEquivalentJoin.getNonNull().toSymbolReference()));
    }
}
