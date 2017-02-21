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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.ScalarAggregationToJoinRewriter;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.Predicates.isInstanceOfAny;
import static com.facebook.presto.sql.planner.optimizations.ScalarQueryUtil.isScalar;
import static java.util.Objects.requireNonNull;

public class TransformCorrelatedScalarAggregationToJoin
        implements Rule
{
    private final FunctionRegistry functionRegistry;

    public TransformCorrelatedScalarAggregationToJoin(FunctionRegistry functionRegistry)
    {
        this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry is null");
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof ApplyNode)) {
            return Optional.empty();
        }

        ApplyNode applyNode = (ApplyNode) node;
        PlanNode subquery = lookup.resolve(applyNode.getSubquery());

        if (applyNode.getCorrelation().isEmpty() || !(isScalar(subquery, lookup) && applyNode.isSubqueryResolved())) {
            return Optional.empty();
        }

        Optional<AggregationNode> aggregation = findAggregation(subquery, lookup);
        if (!(aggregation.isPresent() && aggregation.get().getGroupingKeys().isEmpty())) {
            return Optional.empty();
        }

        ScalarAggregationToJoinRewriter rewriter = new ScalarAggregationToJoinRewriter(functionRegistry, symbolAllocator, idAllocator, lookup);

        PlanNode rewrittenNode = rewriter.rewriteScalarAggregation(applyNode, aggregation.get());

        if (rewrittenNode instanceof ApplyNode) {
            return Optional.empty();
        }

        return Optional.of(rewrittenNode);
    }

    private static Optional<AggregationNode> findAggregation(PlanNode rootNode, Lookup lookup)
    {
        return searchFrom(rootNode, lookup)
                .where(AggregationNode.class::isInstance)
                .skipOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                .findFirst();
    }
}
