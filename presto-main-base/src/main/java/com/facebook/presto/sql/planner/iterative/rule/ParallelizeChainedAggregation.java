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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isEnableParallelizeChainedAggregations;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;

/**
 * Detects chained aggregations where the outer grouping keys are a subset of inner grouping keys
 * and inserts a local ROUND_ROBIN exchange between the outer PARTIAL and the chain leading to the
 * inner FINAL. This parallelizes the outer PARTIAL across local drivers when the inner aggregation's
 * parallelism is below what the local node can support — common when the inner grouping keys have
 * low cardinality and/or the outer aggregation is CPU-heavy (e.g. {@code approx_percentile}).
 *
 * <p>The rule fires on the <em>outer PARTIAL</em> aggregation, since that is what directly consumes
 * the inner aggregation's output. In a typical distributed plan the outer PARTIAL sits in the same
 * fragment as the inner FINAL, with at most one or more {@code ProjectNode}s in between:
 * <pre>
 * - Aggregation (PARTIAL, keys=[k2])         // matched here
 *   - (Project*)
 *     - Aggregation (FINAL, keys=[k1, k2])
 * </pre>
 *
 * <p>Rewritten plan:
 * <pre>
 * - Aggregation (PARTIAL, keys=[k2])
 *   - LocalExchange (ROUND_ROBIN)            // added
 *     - (Project*)
 *       - Aggregation (FINAL, keys=[k1, k2])
 * </pre>
 *
 * <p>The walk-down also tolerates intervening {@code ExchangeNode}s, which can appear in
 * non-standard plan shapes; it stops as soon as it reaches any other node kind.
 *
 * <p>Requirements:
 * <ul>
 *   <li>outer keys must be a strict subset of inner keys (otherwise this is not a cascading shape)</li>
 *   <li>nothing other than Projects and Exchanges may sit between the outer PARTIAL and the inner FINAL —
 *       a Filter or Join in between would change semantics</li>
 *   <li>intermediate Project/Exchange nodes must have exactly one source (multi-source nodes like UNION
 *       ALL Exchanges are not safe to traverse)</li>
 * </ul>
 *
 * <p>Note: this rule also fires when the outer aggregation is global (no GROUP BY) — the empty set
 * is a subset of any non-empty inner key set. For cheap outer functions like {@code sum}/{@code count},
 * the round-robin overhead may exceed the parallelism benefit; the session-property gate is the
 * primary tuning lever.
 */
public class ParallelizeChainedAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnableParallelizeChainedAggregations(session);
    }

    @Override
    public Result apply(AggregationNode outerAggregation, Captures captures, Context context)
    {
        if (outerAggregation.getStep() != PARTIAL) {
            return Result.empty();
        }

        Lookup lookup = context.getLookup();
        PlanNode resolvedSource = lookup.resolve(outerAggregation.getSource());

        // Idempotency: if we've already inserted a local round-robin exchange here, do not fire again.
        if (isLocalRoundRobinExchange(resolvedSource)) {
            return Result.empty();
        }

        // Walk down through Projects and Exchanges to find the inner Aggregation. Other node kinds
        // (Filter, Join, etc.) terminate the walk because they would change semantics. Multi-source
        // nodes (e.g. UNION ALL Exchanges) also terminate the walk — picking sources[0] would
        // silently skip the other sources.
        PlanNode current = resolvedSource;
        while (current instanceof ProjectNode || current instanceof ExchangeNode) {
            if (current.getSources().size() != 1) {
                return Result.empty();
            }
            current = lookup.resolve(current.getSources().get(0));
        }
        if (!(current instanceof AggregationNode)) {
            return Result.empty();
        }
        AggregationNode innerAggregation = (AggregationNode) current;

        if (innerAggregation.getStep() != FINAL) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> outerKeys = new HashSet<>(outerAggregation.getGroupingKeys());
        Set<VariableReferenceExpression> innerKeys = new HashSet<>(innerAggregation.getGroupingKeys());

        if (!innerKeys.containsAll(outerKeys)) {
            return Result.empty();
        }
        // If keys are equal, this is not a cascading aggregation.
        if (outerKeys.equals(innerKeys)) {
            return Result.empty();
        }

        PlanNode roundRobin = roundRobinExchange(context.getIdAllocator().getNextId(), LOCAL, outerAggregation.getSource());
        return Result.ofPlanNode(outerAggregation.replaceChildren(ImmutableList.of(roundRobin)));
    }

    private static boolean isLocalRoundRobinExchange(PlanNode node)
    {
        if (!(node instanceof ExchangeNode)) {
            return false;
        }
        ExchangeNode exchange = (ExchangeNode) node;
        return exchange.getScope() == LOCAL
                && exchange.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_ARBITRARY_DISTRIBUTION);
    }
}
