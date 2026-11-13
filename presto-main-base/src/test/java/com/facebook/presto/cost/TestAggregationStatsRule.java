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

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.FULL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.spi.plan.JoinType.RIGHT;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertEquals;

public class TestAggregationStatsRule
        extends BaseStatsCalculatorTest
{
    private static final VariableReferenceExpression VARIABLE_X = new VariableReferenceExpression(Optional.empty(), "x", BIGINT);
    private static final VariableReferenceExpression VARIABLE_Y = new VariableReferenceExpression(Optional.empty(), "y", BIGINT);
    private static final VariableReferenceExpression VARIABLE_Z = new VariableReferenceExpression(Optional.empty(), "z", BIGINT);
    private static final VariableReferenceExpression VARIABLE_W = new VariableReferenceExpression(Optional.empty(), "w", BIGINT);
    private static final VariableReferenceExpression VARIABLE_UNIQUE = new VariableReferenceExpression(Optional.empty(), "unique", BIGINT);

    @Test
    public void testAggregationWhenAllStatisticsAreKnown()
    {
        Consumer<PlanNodeStatsAssertion> outputRowCountAndZStatsAreCalculated = check -> check
                .outputRowsCount(15)
                .variableStats(VARIABLE_Z, symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(10)
                        .highValue(15)
                        .distinctValuesCount(4)
                        .nullsFraction(0.2))
                .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                        .lowValue(0)
                        .highValue(3)
                        .distinctValuesCount(3)
                        .nullsFraction(0));

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setDistinctValuesCount(4)
                        .build())
                .check(outputRowCountAndZStatsAreCalculated);

        Consumer<PlanNodeStatsAssertion> outputRowsCountAndZStatsAreNotFullyCalculated = check -> check
                .outputRowsCountUnknown()
                .variableStats(VARIABLE_Z, symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .distinctValuesCountUnknown()
                        .nullsFractionUnknown())
                .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                        .unknownRange()
                        .nullsFractionUnknown()
                        .distinctValuesCountUnknown());

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .setNullsFraction(0.1)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);

        testAggregation(
                VariableStatsEstimate.builder()
                        .setLowValue(10)
                        .setHighValue(15)
                        .build())
                .check(outputRowsCountAndZStatsAreNotFullyCalculated);
    }

    private StatsCalculatorAssertion testAggregation(VariableStatsEstimate zStats)
    {
        return tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .addAggregation(pb.variable("count_on_x", BIGINT), pb.rowExpression("count(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Z, zStats)
                        .build())
                .check(check -> check
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "sum", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "count", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "count_on_x", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(VARIABLE_X, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    @Test
    public void testAggregationStatsCappedToInputRows()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count_on_x", BIGINT), pb.rowExpression("count(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdCappedAtUniqueIdRows()
    {
        // As a decorrelated subquery groups: by the unique id and by columns of the rows it was assigned to, which are
        // on the side of the outer join that is never filled with NULLs
        groupedOnUniqueId(pb -> pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(100));
        groupedOnUniqueId(pb -> pb.join(RIGHT, otherRows(pb), numberedRows(pb), new EquiJoinClause(VARIABLE_Y, VARIABLE_X)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdThroughProjectAndFilterCappedAtUniqueIdRows()
    {
        groupedOnUniqueId(
                pb -> {
                    PlanNode join = pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y));
                    return pb.project(identityAssignments(VARIABLE_UNIQUE, VARIABLE_X), pb.filter(pb.rowExpression("y > 0"), join));
                },
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdThroughExchangeAndPartialAggregationCappedAtUniqueIdRows()
    {
        // As once the aggregation is split into a partial and a final step around an exchange
        groupedOnUniqueId(
                FINAL,
                pb -> {
                    PlanNode partialAggregation = pb.aggregation(ab -> ab
                            .step(PARTIAL)
                            .singleGroupingSet(VARIABLE_UNIQUE, VARIABLE_X)
                            .source(pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y))));
                    return pb.exchange(exchange -> exchange
                            .singleDistributionPartitioningScheme(VARIABLE_UNIQUE, VARIABLE_X)
                            .addSource(partialAggregation)
                            .addInputsSet(VARIABLE_UNIQUE, VARIABLE_X));
                },
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdThroughExchangeWithSeveralSourcesNotCapped()
    {
        // The rows of the second source are not numbered rows
        groupedOnUniqueId(
                pb -> pb.exchange(exchange -> exchange
                        .singleDistributionPartitioningScheme(VARIABLE_UNIQUE, VARIABLE_X)
                        .addSource(pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)))
                        .addInputsSet(VARIABLE_UNIQUE, VARIABLE_X)
                        .addSource(otherRows(pb))
                        .addInputsSet(VARIABLE_Y, VARIABLE_Z)),
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdThroughOtherNodeNotCapped()
    {
        // An EnforceSingleRow is not walked through: it returns a row of NULLs when its source is empty
        groupedOnUniqueId(pb -> pb.enforceSingleRow(pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y))), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdThroughProjectReassigningKeyNotCapped()
    {
        // Above the projection x holds x + 1, not the x of the numbered rows
        groupedOnUniqueId(
                pb -> {
                    PlanNode join = pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y));
                    return pb.project(Assignments.builder().putAll(identityAssignments(VARIABLE_UNIQUE)).put(VARIABLE_X, pb.rowExpression("x + 1")).build(), join);
                },
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdThroughExchangeMappingKeyFromOtherColumnNotCapped()
    {
        // The exchange's x is its source's w
        groupedOnUniqueId(
                pb -> pb.exchange(exchange -> exchange
                        .singleDistributionPartitioningScheme(VARIABLE_UNIQUE, VARIABLE_X, VARIABLE_W)
                        .addSource(pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)))
                        .addInputsSet(VARIABLE_UNIQUE, VARIABLE_W, VARIABLE_X)),
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdThroughAggregationNotKeepingKeyNotCapped()
    {
        // Above the aggregation x holds max(x), not a grouping key passed through
        groupedOnUniqueId(
                pb -> {
                    PlanNode join = pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y));
                    return pb.aggregation(ab -> ab
                            .singleGroupingSet(VARIABLE_UNIQUE)
                            .addAggregation(VARIABLE_X, pb.rowExpression("max(x)"))
                            .source(join));
                },
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
        // A global grouping set adds a row whose keys are NULL
        groupedOnUniqueId(
                pb -> pb.aggregation(ab -> ab
                        .groupingSets(new AggregationNode.GroupingSetDescriptor(ImmutableList.of(VARIABLE_UNIQUE, VARIABLE_X), 2, ImmutableSet.of(1)))
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .source(pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)))),
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdBelowAnotherUniqueIdCappedAtUniqueIdRows()
    {
        groupedOnUniqueId(
                pb -> pb.join(
                        LEFT,
                        pb.assignUniqueId(pb.variable("other_unique"), numberedRows(pb)),
                        otherRows(pb),
                        new EquiJoinClause(VARIABLE_X, VARIABLE_Y)),
                VARIABLE_UNIQUE,
                VARIABLE_X)
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdAndInnerJoinEqualKeyCappedAtUniqueIdRows()
    {
        // The inner join makes y equal to x on every row, with the numbered rows on either side
        groupedOnUniqueId(pb -> pb.join(INNER, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_Y)
                .check(check -> check.outputRowsCount(100));
        groupedOnUniqueId(pb -> pb.join(INNER, otherRows(pb), numberedRows(pb), new EquiJoinClause(VARIABLE_Y, VARIABLE_X)), VARIABLE_UNIQUE, VARIABLE_Y)
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdAndKeyEqualToSeveralColumnsCappedAtUniqueIdRows()
    {
        // y is equal to both x and w
        groupedOnUniqueId(
                pb -> pb.join(INNER, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y), new EquiJoinClause(VARIABLE_W, VARIABLE_Y)),
                VARIABLE_UNIQUE,
                VARIABLE_Y)
                .check(check -> check.outputRowsCount(100));
    }

    @Test
    public void testGroupsOnUniqueIdAndOuterJoinEqualKeyNotCapped()
    {
        // Join clauses are only used as equalities below inner joins
        groupedOnUniqueId(pb -> pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_Y)
                .check(check -> check.outputRowsCount(1000));
        groupedOnUniqueId(pb -> pb.join(RIGHT, otherRows(pb), numberedRows(pb), new EquiJoinClause(VARIABLE_Y, VARIABLE_X)), VARIABLE_UNIQUE, VARIABLE_Y)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdAndOtherJoinSideNotCapped()
    {
        // z comes from the other side of the join and no join clause makes it equal to x, so one id can have several groups
        groupedOnUniqueId(pb -> pb.join(INNER, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_Z)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdAndComputedKeyNotCapped()
    {
        // The search only follows keys that pass through unchanged, and x_plus_one is computed above the numbered rows
        VariableReferenceExpression xPlusOne = new VariableReferenceExpression(Optional.empty(), "x_plus_one", BIGINT);
        groupedOnUniqueId(
                pb -> {
                    PlanNode numbered = numberedRows(pb);
                    PlanNode computed = pb.project(
                            Assignments.builder().putAll(identityAssignments(VARIABLE_UNIQUE, VARIABLE_X)).put(pb.variable("x_plus_one"), pb.rowExpression("x + 1")).build(),
                            numbered);
                    return pb.join(LEFT, computed, otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y));
                },
                VARIABLE_UNIQUE,
                xPlusOne)
                .withSourceStats(PlanNodeStatsEstimate.buildFrom(sourceStats())
                        .addVariableStatistics(xPlusOne, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnNullExtendedUniqueIdNotCapped()
    {
        // Unmatched rows of the other side carry a NULL id, an additional group
        groupedOnUniqueId(pb -> pb.join(RIGHT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
        groupedOnUniqueId(pb -> pb.join(LEFT, otherRows(pb), numberedRows(pb), new EquiJoinClause(VARIABLE_Y, VARIABLE_X)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
        groupedOnUniqueId(pb -> pb.join(FULL, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
        groupedOnUniqueId(pb -> pb.join(FULL, otherRows(pb), numberedRows(pb), new EquiJoinClause(VARIABLE_Y, VARIABLE_X)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsWithoutUniqueIdNotCapped()
    {
        // The cap only applies to aggregations that group on the unique id
        groupedOnUniqueId(pb -> pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_X, VARIABLE_W)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdWithUnknownUniqueIdRowsNotCapped()
    {
        groupedOnUniqueId(pb -> pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_X)
                .withSourceStats(new PlanNodeId("numbered_rows"), PlanNodeStatsEstimate.unknown())
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testGroupsOnUniqueIdEstimatedBelowUniqueIdRowsUnchanged()
    {
        groupedOnUniqueId(pb -> pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_X)
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(60)
                        .addVariableStatistics(VARIABLE_UNIQUE, VariableStatsEstimate.builder().setDistinctValuesCount(60).build())
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                        .build())
                .check(check -> check.outputRowsCount(60));
    }

    @Test
    public void testPartialAggregationOnUniqueIdNotCapped()
    {
        // Partial aggregations forward the row count of their source
        groupedOnUniqueId(PARTIAL, pb -> pb.join(LEFT, numberedRows(pb), otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y)), VARIABLE_UNIQUE, VARIABLE_X)
                .check(check -> check.outputRowsCount(1000));
    }

    @Test
    public void testUniqueIdRowsReadThroughItsReference()
    {
        // During optimization, nodes refer to their sources through group references, whose statistics the memo keeps
        // up to date. Statistics cached for the node a reference resolves to can be outdated.
        PlanBuilder pb = new PlanBuilder(testSessionBuilder().build(), new PlanNodeIdAllocator(), tester().getMetadata());
        PlanNode rows = pb.values(pb.variable("x"), pb.variable("w"));
        PlanNode rowsReference = new GroupReference(Optional.empty(), new PlanNodeId("rows_reference"), 1, rows.getOutputVariables(), Optional.empty());
        PlanNode numbered = new AssignUniqueId(Optional.empty(), new PlanNodeId("unique_id"), rowsReference, pb.variable("unique"));
        PlanNode numberedReference = new GroupReference(Optional.empty(), new PlanNodeId("numbered_reference"), 2, numbered.getOutputVariables(), Optional.empty());
        PlanNode join = pb.join(LEFT, numberedReference, otherRows(pb), new EquiJoinClause(VARIABLE_X, VARIABLE_Y));
        AggregationNode aggregation = pb.aggregation(ab -> ab
                .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                .singleGroupingSet(VARIABLE_UNIQUE, VARIABLE_X)
                .source(join));
        Map<PlanNode, PlanNodeStatsEstimate> statistics = ImmutableMap.of(
                join, sourceStats(),
                rowsReference, PlanNodeStatsEstimate.builder().setOutputRowCount(100).build(),
                rows, PlanNodeStatsEstimate.builder().setOutputRowCount(500).build(),
                numbered, PlanNodeStatsEstimate.builder().setOutputRowCount(500).build());
        Lookup lookup = Lookup.from(group -> Stream.of(group.getGroupId() == 1 ? rows : numbered));
        Optional<PlanNodeStatsEstimate> estimate = new AggregationStatsRule(new StatsNormalizer())
                .calculate(aggregation, statistics::get, lookup, testSessionBuilder().build(), pb.getTypes());
        assertEquals(estimate.get().getOutputRowCount(), 100.0);
    }

    private StatsCalculatorAssertion groupedOnUniqueId(Function<PlanBuilder, PlanNode> source, VariableReferenceExpression... groupingKeys)
    {
        return groupedOnUniqueId(SINGLE, source, groupingKeys);
    }

    // Aggregation step on the given grouping keys over the given source, with the statistics of sourceStats(). The
    // numbered rows have 100 rows.
    private StatsCalculatorAssertion groupedOnUniqueId(Step step, Function<PlanBuilder, PlanNode> source, VariableReferenceExpression... groupingKeys)
    {
        return tester().assertStatsFor(pb -> {
            PlanNode sourceNode = source.apply(pb);
            return pb.aggregation(ab -> ab
                    .step(step)
                    .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                    .singleGroupingSet(groupingKeys)
                    .source(sourceNode));
        })
                .withSourceStats(sourceStats())
                .withSourceStats(new PlanNodeId("numbered_rows"), PlanNodeStatsEstimate.builder().setOutputRowCount(100).build());
    }

    // 1000 rows with 100 distinct ids and 50 distinct x, 30 distinct w, 80 distinct y and 40 distinct z values, so
    // multiplying the distinct counts of the grouping keys reaches the 1000-row cap
    private static PlanNodeStatsEstimate sourceStats()
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1000)
                .addVariableStatistics(VARIABLE_UNIQUE, VariableStatsEstimate.builder().setDistinctValuesCount(100).build())
                .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder().setDistinctValuesCount(50).build())
                .addVariableStatistics(VARIABLE_W, VariableStatsEstimate.builder().setDistinctValuesCount(30).build())
                .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder().setDistinctValuesCount(80).build())
                .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder().setDistinctValuesCount(40).build())
                .build();
    }

    // Rows with columns x and w, numbered by an AssignUniqueId with the id column unique
    private static PlanNode numberedRows(PlanBuilder pb)
    {
        PlanNode rows = pb.values(new PlanNodeId("numbered_rows"), pb.variable("x"), pb.variable("w"));
        return new AssignUniqueId(Optional.empty(), new PlanNodeId("unique_id"), rows, pb.variable("unique"));
    }

    // Rows with columns y and z
    private static PlanNode otherRows(PlanBuilder pb)
    {
        return pb.values(pb.variable("y"), pb.variable("z"));
    }

    /**
     * Verifies that a global aggregation (no grouping keys) always produces
     * exactly one output row with FACT confidence level, regardless of the
     * input statistics.
     */
    @Test
    public void testGlobalAggregationReturnsOneRow()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .globalGrouping()
                        .source(pb.values(pb.variable("x", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(100)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .confident(FACT)
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "sum", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression(Optional.empty(), "count", BIGINT), symbolStatsAssertion -> symbolStatsAssertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    /**
     * Verifies that a global aggregation with zero input rows still produces
     * exactly one output row with FACT confidence. This is the expected behavior
     * for queries like {@code SELECT count(*) FROM empty_table}.
     */
    @Test
    public void testGlobalAggregationWithZeroInputRows()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .globalGrouping()
                        .source(pb.values(pb.variable("x", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(0)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setDistinctValuesCount(0)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .confident(FACT));
    }

    /**
     * Verifies that a PARTIAL aggregation step does not reduce the estimated
     * row count. The rule pessimistically assumes no reduction for partial
     * aggregations and forwards the source row count directly.
     */
    @Test
    public void testPartialAggregationPreservesSourceRowCount()
    {
        double sourceRowCount = 500;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .step(AggregationNode.Step.PARTIAL)
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(sourceRowCount)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(3)
                                .distinctValuesCount(3)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that an INTERMEDIATE aggregation step behaves identically to a
     * PARTIAL step: no reduction in estimated row count, source stats forwarded.
     */
    @Test
    public void testIntermediateAggregationPreservesSourceRowCount()
    {
        double sourceRowCount = 500;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .step(AggregationNode.Step.INTERMEDIATE)
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(sourceRowCount)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(3)
                                .distinctValuesCount(3)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that for a SINGLE-step aggregation with a single grouping key,
     * the output row count equals the distinct value count of the grouping key.
     * Also verifies that the grouping key's nulls fraction is set to zero when
     * the source has no nulls in that column.
     */
    @Test
    public void testSingleGroupingKeyNoNulls()
    {
        // y has 10 distinct values with no nulls, source has 200 rows
        // Expected output: 10 rows (= NDV of y)
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(200)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(50)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(9)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(9)
                                .distinctValuesCount(10)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that when a grouping key has a non-zero nulls fraction, the
     * output row count accounts for the null group (NDV + 1 for the null row).
     * Also checks that the resulting nulls fraction for the grouping key is
     * adjusted to {@code 1 / (NDV + 1)}.
     */
    @Test
    public void testSingleGroupingKeyWithNulls()
    {
        // y has 10 distinct values with 20% nulls, source has 200 rows
        // Expected output: 10 + 1 = 11 rows (NDV + 1 for null group)
        // Expected nulls fraction: 1 / (10 + 1) = 1/11
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(200)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(50)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(9)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(11)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(9)
                                .distinctValuesCount(10)
                                .nullsFraction(1.0 / 11)));
    }

    /**
     * Verifies the row count estimate for multiple grouping keys with nulls.
     * The output row count is the product of (NDV + null_row) for each key,
     * capped at the source row count. The nulls fractions of grouping keys
     * are each adjusted to {@code 1 / (NDV + 1)}.
     */
    @Test
    public void testMultipleGroupingKeysWithNulls()
    {
        // y: NDV=3, nullsFraction=0.1 -> contributes 3+1=4
        // z: NDV=5, nullsFraction=0.2 -> contributes 5+1=6
        // Product = 4 * 6 = 24, source has 200 rows, so 24 is used
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(200)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(2)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(14)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(24)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(2)
                                .distinctValuesCount(3)
                                .nullsFraction(1.0 / 4))
                        .variableStats(VARIABLE_Z, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(10)
                                .highValue(14)
                                .distinctValuesCount(5)
                                .nullsFraction(1.0 / 6)));
    }

    /**
     * Verifies that when grouping key statistics are completely unknown
     * (all NaN), the output row count estimate is also unknown. This mirrors
     * how join stats handle missing column statistics.
     */
    @Test
    public void testAggregationWithUnknownGroupingKeyStats()
    {
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.unknown())
                        .build())
                .check(check -> check
                        .outputRowsCountUnknown()
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .unknownRange()
                                .distinctValuesCountUnknown()
                                .nullsFractionUnknown()));
    }

    /**
     * Verifies that a FINAL-step aggregation with a single grouping key
     * produces the same estimates as a SINGLE-step aggregation, since both
     * are handled by the same {@code groupBy} code path.
     */
    @Test
    public void testFinalAggregationMatchesSingleStep()
    {
        double sourceRowCount = 500;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("sum", BIGINT), pb.rowExpression("sum(x)"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .step(AggregationNode.Step.FINAL)
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(19)
                                .setDistinctValuesCount(20)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(20)
                        .variableStats(VARIABLE_Y, symbolStatsAssertion -> symbolStatsAssertion
                                .lowValue(0)
                                .highValue(19)
                                .distinctValuesCount(20)
                                .nullsFraction(0)));
    }

    /**
     * Verifies that a partial aggregation with a global grouping (no grouping keys)
     * preserves the full source row count, since partial aggregations assume
     * pessimistic (no) reduction.
     */
    @Test
    public void testPartialGlobalAggregationPreservesSourceRows()
    {
        double sourceRowCount = 300;
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .globalGrouping()
                        .step(AggregationNode.Step.PARTIAL)
                        .source(pb.values(pb.variable("x", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(sourceRowCount)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(100)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(sourceRowCount));
    }

    /**
     * Verifies that the aggregation output row count is correctly capped at the
     * source row count when multiple grouping keys with no nulls produce a
     * product of NDVs that exceeds the number of input rows.
     */
    @Test
    public void testMultipleGroupingKeysCappedToInputRows()
    {
        // y: NDV=50, no nulls -> contributes 50
        // z: NDV=50, no nulls -> contributes 50
        // Product = 50 * 50 = 2500, but source has only 100 rows => capped to 100
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT), pb.variable("z", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT), pb.variable("z", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(49)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Z, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(49)
                                .setDistinctValuesCount(50)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check.outputRowsCount(100));
    }

    /**
     * Verifies that aggregation statistics are correctly computed for a
     * SINGLE-step aggregation when the grouping key has a high NDV but the
     * source row count is low. The output row count should equal the source
     * row count since NDV cannot exceed it.
     */
    @Test
    public void testGroupingKeyNdvExceedsSourceRows()
    {
        // y: NDV=200, no nulls, but source only has 50 rows => capped to 50
        tester().assertStatsFor(pb -> pb
                .registerVariable(pb.variable("x"))
                .aggregation(ab -> ab
                        .addAggregation(pb.variable("count", BIGINT), pb.rowExpression("count()"))
                        .singleGroupingSet(pb.variable("y", BIGINT))
                        .source(pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT)))))
                .withSourceStats(PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(50)
                        .addVariableStatistics(VARIABLE_X, VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(VARIABLE_Y, VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(199)
                                .setDistinctValuesCount(200)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check -> check.outputRowsCount(50));
    }
}
