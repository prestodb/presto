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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.isJoinSpillingEnabled;
import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.defaultParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.exactlyPartitionedOn;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.singleStream;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.systemPartitionedExchange;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;

public class JoinSwappingUtils
{
    static final List<Class<? extends PlanNode>> EXPANDING_NODE_CLASSES = ImmutableList.of(JoinNode.class, UnnestNode.class);
    private static final double SIZE_DIFFERENCE_THRESHOLD = 8;

    private JoinSwappingUtils() {}

    public static Optional<JoinNode> createRuntimeSwappedJoinNode(
            JoinNode joinNode,
            Metadata metadata,
            SqlParser parser,
            Lookup lookup,
            Session session,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator)
    {
        JoinNode swapped = joinNode.flipChildren();

        PlanNode newLeft = swapped.getLeft();
        Optional<VariableReferenceExpression> leftHashVariable = swapped.getLeftHashVariable();
        // Remove unnecessary LocalExchange in the current probe side. If the immediate left child (new probe side) of the join node
        // is a localExchange, there are two cases: an Exchange introduced by the current probe side (previous build side); or it is a UnionNode.
        // If the exchangeNode has more than 1 sources, it corresponds to the second case, otherwise it corresponds to the first case and could be safe to remove
        PlanNode resolvedSwappedLeft = lookup.resolve(newLeft);
        if (resolvedSwappedLeft instanceof ExchangeNode && resolvedSwappedLeft.getSources().size() == 1) {
            // Ensure the new probe after skipping the local exchange will satisfy the required probe side property
            if (checkProbeSidePropertySatisfied(resolvedSwappedLeft.getSources().get(0), metadata, parser, lookup, session, variableAllocator)) {
                newLeft = resolvedSwappedLeft.getSources().get(0);
                // The HashGenerationOptimizer will generate hashVariables and append to the output layout of the nodes following the same order. Therefore,
                // we use the index of the old hashVariable in the ExchangeNode output layout to retrieve the hashVariable from the new left node, and feed
                // it as the leftHashVariable of the swapped join node.
                if (swapped.getLeftHashVariable().isPresent()) {
                    int hashVariableIndex = resolvedSwappedLeft.getOutputVariables().indexOf(swapped.getLeftHashVariable().get());
                    leftHashVariable = Optional.of(resolvedSwappedLeft.getSources().get(0).getOutputVariables().get(hashVariableIndex));
                    // When join output layout contains new left side's hashVariable (e.g., a nested join in a single stage, the inner join's output layout possibly
                    // carry the join hashVariable from its new probe), after removing the local exchange at the new probe, the output variables of the join node will
                    // also change, which has to be broadcast upwards (rewriting plan nodes) until the point where this hashVariable is no longer the output.
                    // This is against typical iterativeOptimizer behavior and given this case is rare, just abort the swapping for this scenario.
                    if (swapped.getOutputVariables().contains(swapped.getLeftHashVariable().get())) {
                        return Optional.empty();
                    }
                }
            }
        }

        // Add additional localExchange if the new build side does not satisfy the partitioning conditions.
        List<VariableReferenceExpression> buildJoinVariables = swapped.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getRight)
                .collect(toImmutableList());
        PlanNode newRight = swapped.getRight();
        if (!checkBuildSidePropertySatisfied(swapped.getRight(), buildJoinVariables, metadata, parser, lookup, session, variableAllocator)) {
            if (getTaskConcurrency(session) > 1) {
                newRight = systemPartitionedExchange(
                        idAllocator.getNextId(),
                        LOCAL,
                        swapped.getRight(),
                        buildJoinVariables,
                        swapped.getRightHashVariable());
            }
            else {
                newRight = gatheringExchange(idAllocator.getNextId(), LOCAL, swapped.getRight());
            }
        }

        JoinNode newJoinNode = new JoinNode(
                swapped.getSourceLocation(),
                swapped.getId(),
                swapped.getType(),
                newLeft,
                newRight,
                swapped.getCriteria(),
                swapped.getOutputVariables(),
                swapped.getFilter(),
                leftHashVariable,
                swapped.getRightHashVariable(),
                swapped.getDistributionType(),
                swapped.getDynamicFilters());

        return Optional.of(newJoinNode);
    }

    // Check if the new probe side after removing unnecessary local exchange is valid.
    public static boolean checkProbeSidePropertySatisfied(PlanNode node, Metadata metadata, SqlParser parser, Lookup lookup, Session session, VariableAllocator variableAllocator)
    {
        StreamPreferredProperties requiredProbeProperty;
        if (isSpillEnabled(session) && isJoinSpillingEnabled(session)) {
            requiredProbeProperty = fixedParallelism();
        }
        else {
            requiredProbeProperty = defaultParallelism(session);
        }
        StreamPropertyDerivations.StreamProperties nodeProperty = derivePropertiesRecursively(node, metadata, parser, lookup, session, variableAllocator);
        return requiredProbeProperty.isSatisfiedBy(nodeProperty);
    }

    // Check if the property of a planNode satisfies the requirements for directly feeding as the build side of a JoinNode.
    private static boolean checkBuildSidePropertySatisfied(
            PlanNode node,
            List<VariableReferenceExpression> partitioningColumns,
            Metadata metadata,
            SqlParser parser,
            Lookup lookup,
            Session session,
            VariableAllocator variableAllocator)
    {
        StreamPreferredProperties requiredBuildProperty;
        if (getTaskConcurrency(session) > 1) {
            requiredBuildProperty = exactlyPartitionedOn(partitioningColumns);
        }
        else {
            requiredBuildProperty = singleStream();
        }
        StreamPropertyDerivations.StreamProperties nodeProperty = derivePropertiesRecursively(node, metadata, parser, lookup, session, variableAllocator);
        return requiredBuildProperty.isSatisfiedBy(nodeProperty);
    }

    private static StreamPropertyDerivations.StreamProperties derivePropertiesRecursively(
            PlanNode node,
            Metadata metadata,
            SqlParser parser,
            Lookup lookup,
            Session session,
            VariableAllocator variableAllocator)
    {
        PlanNode actual = lookup.resolve(node);
        List<StreamPropertyDerivations.StreamProperties> inputProperties = actual.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, parser, lookup, session, variableAllocator))
                .collect(toImmutableList());
        return StreamPropertyDerivations.deriveProperties(actual, inputProperties, metadata, session, TypeProvider.viewOf(variableAllocator.getVariables()), parser);
    }

    public static boolean isBelowBroadcastLimit(PlanNode planNode, Rule.Context context)
    {
        DataSize joinMaxBroadcastTableSize = getJoinMaxBroadcastTableSize(context.getSession());
        return DetermineJoinDistributionType.getSourceTablesSizeInBytes(planNode, context) <= joinMaxBroadcastTableSize.toBytes();
    }

    public static boolean isSmallerThanThreshold(PlanNode planNodeA, PlanNode planNodeB, Rule.Context context)
    {
        double aOutputSize = getFirstKnownOutputSizeInBytes(planNodeA, context);
        double bOutputSize = getFirstKnownOutputSizeInBytes(planNodeB, context);
        return aOutputSize * SIZE_DIFFERENCE_THRESHOLD < bOutputSize;
    }

    private static double getFirstKnownOutputSizeInBytes(PlanNode node, Rule.Context context)
    {
        return getFirstKnownOutputSizeInBytes(node, context.getLookup(), context.getStatsProvider());
    }

    /**
     * Recursively looks for the first source node with a known estimate and uses that to return an approximate output size.
     * Returns NaN if an un-estimated expanding node (Join or Unnest) is encountered.
     * The amount of reduction in size from un-estimated non-expanding nodes (e.g. an un-estimated filter or aggregation)
     * is not accounted here. We make use of the first available estimate and make decision about flipping join sides only if
     * we find a large difference in output size of both sides.
     */
    @VisibleForTesting
    public static double getFirstKnownOutputSizeInBytes(PlanNode node, Lookup lookup, StatsProvider statsProvider)
    {
        return Stream.of(node)
                .flatMap(planNode -> {
                    if (planNode instanceof GroupReference) {
                        return lookup.resolveGroup(node);
                    }
                    return Stream.of(planNode);
                })
                .mapToDouble(resolvedNode -> {
                    double outputSizeInBytes = statsProvider.getStats(resolvedNode).getOutputSizeInBytes(resolvedNode);
                    if (!isNaN(outputSizeInBytes)) {
                        return outputSizeInBytes;
                    }

                    if (EXPANDING_NODE_CLASSES.stream().anyMatch(clazz -> clazz.isInstance(resolvedNode))) {
                        return NaN;
                    }

                    List<PlanNode> sourceNodes = resolvedNode.getSources();
                    if (sourceNodes.isEmpty()) {
                        return NaN;
                    }

                    double sourcesOutputSizeInBytes = 0;
                    for (PlanNode sourceNode : sourceNodes) {
                        double firstKnownOutputSizeInBytes = getFirstKnownOutputSizeInBytes(sourceNode, lookup, statsProvider);
                        if (isNaN(firstKnownOutputSizeInBytes)) {
                            return NaN;
                        }
                        sourcesOutputSizeInBytes += firstKnownOutputSizeInBytes;
                    }
                    return sourcesOutputSizeInBytes;
                })
                .sum();
    }
}
