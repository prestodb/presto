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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ScalarAggregationToJoinRewriter;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;

import java.util.Optional;

import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.LateralJoin.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
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

    private final FunctionAndTypeManager functionAndTypeManager;

    public TransformCorrelatedScalarAggregationToJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
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

        ScalarAggregationToJoinRewriter rewriter = new ScalarAggregationToJoinRewriter(functionAndTypeManager, context.getVariableAllocator(), context.getIdAllocator(), context.getLookup());

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
}
