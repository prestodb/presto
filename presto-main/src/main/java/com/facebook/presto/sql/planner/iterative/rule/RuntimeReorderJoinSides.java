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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties;
import com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getTaskConcurrency;
import static com.facebook.presto.SystemSessionProperties.isJoinSpillingEnabled;
import static com.facebook.presto.SystemSessionProperties.isSpillEnabled;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.defaultParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.exactlyPartitionedOn;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.fixedParallelism;
import static com.facebook.presto.sql.planner.optimizations.StreamPreferredProperties.singleStream;
import static com.facebook.presto.sql.planner.optimizations.StreamPropertyDerivations.StreamProperties;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.systemPartitionedExchange;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RuntimeReorderJoinSides
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(RuntimeReorderJoinSides.class);

    private static final Pattern<JoinNode> PATTERN = join();

    private final Metadata metadata;
    private final SqlParser parser;

    public RuntimeReorderJoinSides(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        // Early exit if the leaves of the joinNode subtree include non tableScan nodes.
        if (searchFrom(joinNode, context.getLookup())
                .where(node -> node.getSources().isEmpty() && !(node instanceof TableScanNode))
                .matches()) {
            return Result.empty();
        }

        double leftOutputSizeInBytes = Double.NaN;
        double rightOutputSizeInBytes = Double.NaN;
        StatsProvider statsProvider = context.getStatsProvider();
        if (searchFrom(joinNode, context.getLookup())
                .where(node -> !(node instanceof TableScanNode) && !(node instanceof ExchangeNode))
                .findAll().size() == 1) {
            // Simple plan is characterized as Join directly on tableScanNodes only with exchangeNode in between.
            // For simple plans, directly fetch the overall table sizes as the size of the join sides to have
            // accurate input bytes statistics and meanwhile avoid non-negligible cost of collecting and processing
            // per-column statistics.
            leftOutputSizeInBytes = statsProvider.getStats(joinNode.getLeft()).getOutputSizeInBytes();
            rightOutputSizeInBytes = statsProvider.getStats(joinNode.getRight()).getOutputSizeInBytes();
        }
        if (Double.isNaN(leftOutputSizeInBytes) || Double.isNaN(rightOutputSizeInBytes)) {
            // Per-column estimate left and right output size for complex plans or when size statistics is unavailable.
            leftOutputSizeInBytes = statsProvider.getStats(joinNode.getLeft()).getOutputSizeInBytes(joinNode.getLeft().getOutputVariables());
            rightOutputSizeInBytes = statsProvider.getStats(joinNode.getRight()).getOutputSizeInBytes(joinNode.getRight().getOutputVariables());
        }

        if (Double.isNaN(leftOutputSizeInBytes) || Double.isNaN(rightOutputSizeInBytes)) {
            return Result.empty();
        }
        if (rightOutputSizeInBytes <= leftOutputSizeInBytes) {
            return Result.empty();
        }

        // Check if the swapped join is valid.
        if (!isSwappedJoinValid(joinNode)) {
            return Result.empty();
        }
        JoinNode swapped = joinNode.flipChildren();

        PlanNode newLeft = swapped.getLeft();
        Optional<VariableReferenceExpression> leftHashVariable = swapped.getLeftHashVariable();
        // Remove unnecessary LocalExchange in the current probe side. If the immediate left child (new probe side) of the join node
        // is a localExchange, there are two cases: an Exchange introduced by the current probe side (previous build side); or it is a UnionNode.
        // If the exchangeNode has more than 1 sources, it corresponds to the second case, otherwise it corresponds to the first case and could be safe to remove
        PlanNode resolvedSwappedLeft = context.getLookup().resolve(newLeft);
        if (resolvedSwappedLeft instanceof ExchangeNode && resolvedSwappedLeft.getSources().size() == 1) {
            // Ensure the new probe after skipping the local exchange will satisfy the required probe side property
            if (checkProbeSidePropertySatisfied(resolvedSwappedLeft.getSources().get(0), context)) {
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
                        return Result.empty();
                    }
                }
            }
        }

        // Add additional localExchange if the new build side does not satisfy the partitioning conditions.
        List<VariableReferenceExpression> buildJoinVariables = swapped.getCriteria().stream()
                .map(JoinNode.EquiJoinClause::getRight)
                .collect(toImmutableList());
        PlanNode newRight = swapped.getRight();
        if (!checkBuildSidePropertySatisfied(swapped.getRight(), buildJoinVariables, context)) {
            if (getTaskConcurrency(context.getSession()) > 1) {
                newRight = systemPartitionedExchange(
                        context.getIdAllocator().getNextId(),
                        LOCAL,
                        swapped.getRight(),
                        buildJoinVariables,
                        swapped.getRightHashVariable());
            }
            else {
                newRight = gatheringExchange(context.getIdAllocator().getNextId(), LOCAL, swapped.getRight());
            }
        }

        JoinNode newJoinNode = new JoinNode(
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

        log.debug(format("Probe size: %.2f is smaller than Build size: %.2f => invoke runtime join swapping on JoinNode ID: %s.", leftOutputSizeInBytes, rightOutputSizeInBytes, newJoinNode.getId()));
        return Result.ofPlanNode(newJoinNode);
    }

    private boolean isSwappedJoinValid(JoinNode join)
    {
        return !(join.getDistributionType().get() == REPLICATED && join.getType() == LEFT) &&
                !(join.getDistributionType().get() == PARTITIONED && join.getCriteria().isEmpty() && join.getType() == RIGHT);
    }

    // Check if the new probe side after removing unnecessary local exchange is valid.
    private boolean checkProbeSidePropertySatisfied(PlanNode node, Context context)
    {
        StreamPreferredProperties requiredProbeProperty;
        if (isSpillEnabled(context.getSession()) && isJoinSpillingEnabled(context.getSession())) {
            requiredProbeProperty = fixedParallelism();
        }
        else {
            requiredProbeProperty = defaultParallelism(context.getSession());
        }
        StreamProperties nodeProperty = derivePropertiesRecursively(node, metadata, parser, context);
        return requiredProbeProperty.isSatisfiedBy(nodeProperty);
    }

    // Check if the property of a planNode satisfies the requirements for directly feeding as the build side of a JoinNode.
    private boolean checkBuildSidePropertySatisfied(PlanNode node, List<VariableReferenceExpression> partitioningColumns, Context context)
    {
        StreamPreferredProperties requiredBuildProperty;
        if (getTaskConcurrency(context.getSession()) > 1) {
            requiredBuildProperty = exactlyPartitionedOn(partitioningColumns);
        }
        else {
            requiredBuildProperty = singleStream();
        }
        StreamProperties nodeProperty = derivePropertiesRecursively(node, metadata, parser, context);
        return requiredBuildProperty.isSatisfiedBy(nodeProperty);
    }

    private StreamProperties derivePropertiesRecursively(PlanNode node, Metadata metadata, SqlParser parser, Context context)
    {
        PlanNode actual = context.getLookup().resolve(node);
        List<StreamProperties> inputProperties = actual.getSources().stream()
                .map(source -> derivePropertiesRecursively(source, metadata, parser, context))
                .collect(toImmutableList());
        return StreamPropertyDerivations.deriveProperties(actual, inputProperties, metadata, context.getSession(), context.getVariableAllocator().getTypes(), parser);
    }
}
