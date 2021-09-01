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
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.isEnableIntermediateAggregations;
import static com.facebook.presto.matching.Pattern.empty;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.roundRobinExchange;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.groupingColumns;
import static com.facebook.presto.sql.planner.plan.Patterns.Aggregation.step;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.getOnlyElement;

/**
 * Adds INTERMEDIATE aggregations between an un-grouped FINAL aggregation and its preceding
 * PARTIAL aggregation.
 * <p>
 * From:
 * <pre>
 * - Aggregation (FINAL)
 *   - RemoteExchange (GATHER)
 *     - Aggregation (PARTIAL)
 * </pre>
 * To:
 * <pre>
 * - Aggregation (FINAL)
 *   - LocalExchange (GATHER)
 *     - Aggregation (INTERMEDIATE)
 *       - LocalExchange (ARBITRARY)
 *         - RemoteExchange (GATHER)
 *           - Aggregation (INTERMEDIATE)
 *             - LocalExchange (GATHER)
 *               - Aggregation (PARTIAL)
 * </pre>
 * <p>
 */
public class AddIntermediateAggregations
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            // Only consider FINAL un-grouped aggregations
            .with(step().equalTo(FINAL))
            .with(empty(groupingColumns()))
            // Only consider aggregations without ORDER BY clause
            .matching(node -> !node.hasOrderings());

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isEnableIntermediateAggregations(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        Lookup lookup = context.getLookup();
        PlanNodeIdAllocator idAllocator = context.getIdAllocator();
        Session session = context.getSession();
        TypeProvider types = context.getVariableAllocator().getTypes();

        Optional<PlanNode> rewrittenSource = recurseToPartial(lookup.resolve(aggregation.getSource()), lookup, idAllocator, types);

        if (!rewrittenSource.isPresent()) {
            return Result.empty();
        }

        PlanNode source = rewrittenSource.get();

        if (getTaskConcurrency(session) > 1) {
            Map<VariableReferenceExpression, Aggregation> variableToAggregations = inputsAsOutputs(aggregation.getAggregations(), types);

            if (variableToAggregations.isEmpty()) {
                return Result.empty();
            }

            source = roundRobinExchange(idAllocator.getNextId(), LOCAL, source);
            source = new AggregationNode(
                    idAllocator.getNextId(),
                    source,
                    variableToAggregations,
                    aggregation.getGroupingSets(),
                    aggregation.getPreGroupedVariables(),
                    INTERMEDIATE,
                    aggregation.getHashVariable(),
                    aggregation.getGroupIdVariable());
            source = gatheringExchange(idAllocator.getNextId(), LOCAL, source);
        }

        return Result.ofPlanNode(aggregation.replaceChildren(ImmutableList.of(source)));
    }

    /**
     * Recurse through a series of preceding ExchangeNodes and ProjectNodes to find the preceding PARTIAL aggregation
     */
    private Optional<PlanNode> recurseToPartial(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, TypeProvider types)
    {
        if (node instanceof AggregationNode && ((AggregationNode) node).getStep() == PARTIAL) {
            return Optional.of(addGatheringIntermediate((AggregationNode) node, idAllocator, types));
        }

        if (!(node instanceof ExchangeNode) && !(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ImmutableList.Builder<PlanNode> builder = ImmutableList.builder();
        for (PlanNode source : node.getSources()) {
            Optional<PlanNode> planNode = recurseToPartial(lookup.resolve(source), lookup, idAllocator, types);
            if (!planNode.isPresent()) {
                return Optional.empty();
            }
            builder.add(planNode.get());
        }
        return Optional.of(node.replaceChildren(builder.build()));
    }

    private PlanNode addGatheringIntermediate(AggregationNode aggregation, PlanNodeIdAllocator idAllocator, TypeProvider types)
    {
        verify(aggregation.getGroupingKeys().isEmpty(), "Should be an un-grouped aggregation");
        ExchangeNode gatheringExchange = gatheringExchange(idAllocator.getNextId(), LOCAL, aggregation);
        return new AggregationNode(
                idAllocator.getNextId(),
                gatheringExchange,
                outputsAsInputs(aggregation.getAggregations()),
                aggregation.getGroupingSets(),
                aggregation.getPreGroupedVariables(),
                INTERMEDIATE,
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable());
    }

    /**
     * Rewrite assignments so that inputs are in terms of the output symbols.
     * <p>
     * Example:
     * 'a' := sum('b') => 'a' := sum('a')
     * 'a' := count(*) => 'a' := count('a')
     */
    private static Map<VariableReferenceExpression, Aggregation> outputsAsInputs(Map<VariableReferenceExpression, Aggregation> assignments)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> builder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : assignments.entrySet()) {
            VariableReferenceExpression output = entry.getKey();
            Aggregation aggregation = entry.getValue();
            checkState(!aggregation.getOrderBy().isPresent(), "Intermediate aggregation does not support ORDER BY");
            builder.put(
                    output,
                    new Aggregation(
                            new CallExpression(
                                    aggregation.getCall().getDisplayName(),
                                    aggregation.getCall().getFunctionHandle(),
                                    aggregation.getCall().getType(),
                                    ImmutableList.of(output)),
                            Optional.empty(),
                            Optional.empty(),
                            false,
                            Optional.empty()));  // No mask for INTERMEDIATE
        }
        return builder.build();
    }

    /**
     * Rewrite assignments so that outputs are in terms of the input symbols.
     * This operation only reliably applies to aggregation steps that take partial inputs (e.g. INTERMEDIATE and split FINALs),
     * which are guaranteed to have exactly one input and one output.
     * <p>
     * Example:
     * 'a' := sum('b') => 'b' := sum('b')
     */
    private static Map<VariableReferenceExpression, Aggregation> inputsAsOutputs(Map<VariableReferenceExpression, Aggregation> assignments, TypeProvider types)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> builder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : assignments.entrySet()) {
            // Should only have one input symbol
            Aggregation aggregation = entry.getValue();
            if (!(aggregation.getArguments().size() == 1 && !aggregation.getOrderBy().isPresent() && !aggregation.getFilter().isPresent())) {
                return ImmutableMap.of();
            }
            VariableReferenceExpression input = getOnlyElement(extractAggregationUniqueVariables(entry.getValue(), types));
            builder.put(input, entry.getValue());
        }
        return builder.build();
    }
}
