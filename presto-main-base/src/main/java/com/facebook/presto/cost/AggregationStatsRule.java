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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.AggregationNode.Step.INTERMEDIATE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.PropertyDerivations.filterOrRewrite;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static java.lang.Double.NaN;
import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;

public class AggregationStatsRule
        extends SimpleStatsRule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = aggregation();

    public AggregationStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(AggregationNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        if (node.getGroupingSetCount() != 1) {
            return Optional.empty();
        }

        PlanNodeStatsEstimate estimate;

        if (node.getStep() == PARTIAL || node.getStep() == INTERMEDIATE) {
            estimate = partialGroupBy(
                    statsProvider.getStats(node.getSource()),
                    node.getGroupingKeys(),
                    node.getAggregations());
        }
        else {
            estimate = groupBy(
                    statsProvider.getStats(node.getSource()),
                    node.getGroupingKeys(),
                    node.getAggregations());
            // Walk back from the aggregation through nodes that keep the following invariant: each row they output carries the
            // grouping-key tuple of one row of their input (for a join, of one side), so they may drop or repeat tuples but cannot
            // add any. If the walk reaches an AssignUniqueId whose id is a grouping key (as in decorrelated subqueries), each of its
            // rows has its own tuple, so the aggregation returns at most as many rows as the AssignUniqueId has. That count is read
            // from its source, which has the same count and is a memo reference whose statistics stay current during optimization,
            // while those cached for the node the search returns may not
            double maxGroups = findSourceWithOneRowPerGroup(node.getSource(), ImmutableSet.copyOf(node.getGroupingKeys()), lookup)
                    .map(assignUniqueId -> statsProvider.getStats(assignUniqueId.getSource()).getOutputRowCount())
                    .orElse(NaN);
            if (maxGroups < estimate.getOutputRowCount()) {
                estimate = estimate.mapOutputRowCount(rowCount -> maxGroups);
            }
        }
        return Optional.of(estimate);
    }

    public static PlanNodeStatsEstimate groupBy(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables, Map<VariableReferenceExpression, Aggregation> aggregations)
    {
        // Used to estimate FINAL or SINGLE step aggregations
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        if (groupByVariables.isEmpty()) {
            result.setConfidence(FACT);
            result.setOutputRowCount(1);
        }
        else {
            result.addVariableStatistics(getGroupByVariablesStatistics(sourceStats, groupByVariables));
            double rowsCount = getRowsCount(sourceStats, groupByVariables);
            result.setOutputRowCount(min(rowsCount, sourceStats.getOutputRowCount()));
        }

        aggregations.forEach((key, value) -> result.addVariableStatistics(key, estimateAggregationStats(value, sourceStats)));

        return result.build();
    }

    public static double getRowsCount(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables)
    {
        double rowsCount = 1;
        for (VariableReferenceExpression groupByVariable : groupByVariables) {
            VariableStatsEstimate symbolStatistics = sourceStats.getVariableStatistics(groupByVariable);
            int nullRow = (symbolStatistics.getNullsFraction() == 0.0) ? 0 : 1;
            rowsCount *= symbolStatistics.getDistinctValuesCount() + nullRow;
        }
        return rowsCount;
    }

    private static PlanNodeStatsEstimate partialGroupBy(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables, Map<VariableReferenceExpression, Aggregation> aggregations)
    {
        // Pessimistic assumption of no reduction from PARTIAL and INTERMEDIATE aggregation, forwarding of the source statistics.
        // This makes the CBO estimates in the EXPLAIN plan output easier to understand,
        // even though partial aggregations are added after the CBO rules have been run.
        PlanNodeStatsEstimate.Builder result = PlanNodeStatsEstimate.builder();
        result.setOutputRowCount(sourceStats.getOutputRowCount());
        result.addVariableStatistics(getGroupByVariablesStatistics(sourceStats, groupByVariables));
        aggregations.forEach((key, value) -> result.addVariableStatistics(key, estimateAggregationStats(value, sourceStats)));

        return result.build();
    }

    private static Optional<AssignUniqueId> findSourceWithOneRowPerGroup(PlanNode node, Set<VariableReferenceExpression> groupingKeys, Lookup lookup)
    {
        Optional<PlanNode> source = searchFrom(node, lookup)
                .where(planNode -> planNode instanceof JoinNode || hasOneRowPerGroup(planNode, groupingKeys))
                .recurseOnlyWhen(planNode -> addsNoGroups(planNode, groupingKeys))
                .findFirst();
        if (!source.isPresent()) {
            return Optional.empty();
        }
        if (source.get() instanceof AssignUniqueId) {
            // Every node walked through passed all grouping keys through from its source, so they are all columns of the AssignUniqueId
            return Optional.of((AssignUniqueId) source.get());
        }
        JoinNode join = (JoinNode) source.get();
        JoinType type = join.getType();
        List<EquiJoinClause> equalities = type == INNER ? join.getCriteria() : ImmutableList.of();
        Optional<AssignUniqueId> assignUniqueId = Optional.empty();
        if (type == INNER || type == LEFT) {
            assignUniqueId = findSourceWithOneRowPerGroupInJoinSide(join.getLeft(), groupingKeys, equalities, lookup);
        }
        if (!assignUniqueId.isPresent() && (type == INNER || type == RIGHT)) {
            assignUniqueId = findSourceWithOneRowPerGroupInJoinSide(join.getRight(), groupingKeys, equalities, lookup);
        }
        return assignUniqueId;
    }

    private static boolean hasOneRowPerGroup(PlanNode node, Set<VariableReferenceExpression> groupingKeys)
    {
        // Each row of an AssignUniqueId has its own id
        return node instanceof AssignUniqueId && groupingKeys.contains(((AssignUniqueId) node).getIdVariable());
    }

    private static boolean addsNoGroups(PlanNode node, Set<VariableReferenceExpression> groupingKeys)
    {
        // Nodes that only drop, merge or move rows or add columns, when they pass the grouping keys through unchanged, the way
        // PropertyDerivations follows columns through a projection's assignments, an exchange's inputs and an aggregation's keys
        if (node instanceof ProjectNode) {
            Assignments assignments = ((ProjectNode) node).getAssignments();
            return groupingKeys.stream().allMatch(key -> key.equals(assignments.get(key)));
        }
        if (node instanceof ExchangeNode) {
            ExchangeNode exchange = (ExchangeNode) node;
            List<VariableReferenceExpression> outputs = exchange.getOutputVariables();
            return exchange.getSources().size() == 1
                    && groupingKeys.stream().allMatch(key -> outputs.contains(key) && key.equals(exchange.getInputs().get(0).get(outputs.indexOf(key))));
        }
        if (node instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) node;
            return aggregation.getGroupingSetCount() == 1 && aggregation.getGroupingKeys().containsAll(groupingKeys);
        }
        return isInstanceOfAny(FilterNode.class, AssignUniqueId.class).test(node);
    }

    private static Optional<AssignUniqueId> findSourceWithOneRowPerGroupInJoinSide(
            PlanNode side,
            Set<VariableReferenceExpression> groupingKeys,
            List<EquiJoinClause> equalities,
            Lookup lookup)
    {
        ImmutableSet.Builder<VariableReferenceExpression> sideKeys = ImmutableSet.builder();
        for (VariableReferenceExpression key : groupingKeys) {
            Optional<VariableReferenceExpression> sideKey = filterOrRewrite(side.getOutputVariables(), equalities, key);
            if (!sideKey.isPresent()) {
                return Optional.empty();
            }
            sideKeys.add(sideKey.get());
        }
        return findSourceWithOneRowPerGroup(side, sideKeys.build(), lookup);
    }

    private static Map<VariableReferenceExpression, VariableStatsEstimate> getGroupByVariablesStatistics(PlanNodeStatsEstimate sourceStats, Collection<VariableReferenceExpression> groupByVariables)
    {
        ImmutableMap.Builder<VariableReferenceExpression, VariableStatsEstimate> variableStatsEstimates = ImmutableMap.builder();
        for (VariableReferenceExpression groupByVariable : groupByVariables) {
            VariableStatsEstimate symbolStatistics = sourceStats.getVariableStatistics(groupByVariable);
            variableStatsEstimates.put(groupByVariable, symbolStatistics.mapNullsFraction(nullsFraction -> {
                if (nullsFraction == 0.0) {
                    return 0.0;
                }
                return 1.0 / (symbolStatistics.getDistinctValuesCount() + 1);
            }));
        }
        return variableStatsEstimates.build();
    }

    private static VariableStatsEstimate estimateAggregationStats(Aggregation aggregation, PlanNodeStatsEstimate sourceStats)
    {
        requireNonNull(aggregation, "aggregation is null");
        requireNonNull(sourceStats, "sourceStats is null");

        // TODO implement simple aggregations like: min, max, count, sum
        return VariableStatsEstimate.unknown();
    }
}
