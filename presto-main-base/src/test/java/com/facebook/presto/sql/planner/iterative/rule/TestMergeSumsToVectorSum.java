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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Tests for {@link MergeSumsToVectorSum}.
 * <p>
 * Positive tests register a test-only scalar stub named {@code vector_sum} so that the
 * rule's {@code isVectorSumAvailable()} guard passes. In production, {@code vector_sum}
 * is a Velox aggregate function (see D94039944); the stub here satisfies the function
 * registry lookup for plan-level testing without requiring a full aggregate implementation.
 */
public class TestMergeSumsToVectorSum
        extends BaseRuleTest
{
    /**
     * Test-only scalar stubs that register "vector_sum" in the function registry.
     * Each overload handles a specific array element type used in tests.
     */
    public static final class VectorSumTestFunctions
    {
        private VectorSumTestFunctions() {}

        @ScalarFunction("vector_sum")
        @SqlType("array(bigint)")
        public static Block vectorSumBigint(@SqlType("array(bigint)") Block array)
        {
            return array;
        }

        @ScalarFunction("vector_sum")
        @SqlType("array(double)")
        public static Block vectorSumDouble(@SqlType("array(double)") Block array)
        {
            return array;
        }
    }

    @BeforeClass
    public void registerVectorSum()
    {
        getMetadata().getFunctionAndTypeManager().registerBuiltInFunctions(
                extractFunctions(VectorSumTestFunctions.class));
    }

    // ---- Positive tests: rule fires and produces correct plan ----

    @Test
    public void testFiresAboveThreshold()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3", BIGINT), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
    }

    @Test
    public void testFiresAtExactThreshold()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "3")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3", BIGINT), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
    }

    @Test
    public void testFiresWithGroupByKeys()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("key", BIGINT);
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.singleGroupingSet(p.variable("key"))
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3", BIGINT), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("key", BIGINT), p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
        AggregationNode aggNode = (AggregationNode) ((ProjectNode) result).getSource();
        assertEquals(aggNode.getGroupingKeys().size(), 1, "Group-by key should be preserved");
    }

    @Test
    public void testFiresPreservingNonMergeableAggregations()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "3")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3", BIGINT), p.rowExpression("sum(col3)"))
                            .addAggregation(p.variable("max_1", BIGINT), p.rowExpression("max(col1)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
        AggregationNode aggNode = (AggregationNode) ((ProjectNode) result).getSource();
        long maxAggs = aggNode.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getDisplayName().equalsIgnoreCase("max"))
                .count();
        assertEquals(maxAggs, 1, "MAX aggregation should be preserved unchanged");
    }

    @Test
    public void testFiresWithMultipleTypeGroups()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    p.variable("dcol1", DOUBLE);
                    p.variable("dcol2", DOUBLE);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3", BIGINT), p.rowExpression("sum(col3)"))
                            .addAggregation(p.variable("dsum_1", DOUBLE), p.rowExpression("sum(dcol1)"))
                            .addAggregation(p.variable("dsum_2", DOUBLE), p.rowExpression("sum(dcol2)"))
                            .source(p.values(
                                    p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT),
                                    p.variable("dcol1", DOUBLE), p.variable("dcol2", DOUBLE)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 2);
    }

    @Test
    public void testFiresForCountExpressions()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("cnt_1", BIGINT), p.rowExpression("count(col1)"))
                            .addAggregation(p.variable("cnt_2", BIGINT), p.rowExpression("count(col2)"))
                            .addAggregation(p.variable("cnt_3", BIGINT), p.rowExpression("count(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
        ProjectNode outerProject = (ProjectNode) result;
        long elementAtCount = outerProject.getAssignments().getMap().values().stream()
                .filter(expr -> expr instanceof CallExpression
                        && ((CallExpression) expr).getDisplayName().equals("element_at"))
                .count();
        assertEquals(elementAtCount, 3, "Expected 3 element_at projections for 3 COUNT aggregations");
    }

    @Test
    public void testFiresForMixedSumAndCount()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("cnt_1", BIGINT), p.rowExpression("count(col1)"))
                            .addAggregation(p.variable("cnt_2", BIGINT), p.rowExpression("count(col2)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
        AggregationNode aggNode = (AggregationNode) ((ProjectNode) result).getSource();
        assertEquals(aggNode.getAggregations().size(), 1, "All aggregations should be merged into one vector_sum");
        ProjectNode outerProject = (ProjectNode) result;
        long elementAtCount = outerProject.getAssignments().getMap().values().stream()
                .filter(expr -> expr instanceof CallExpression
                        && ((CallExpression) expr).getDisplayName().equals("element_at"))
                .count();
        assertEquals(elementAtCount, 4, "Expected 4 element_at projections (2 SUM + 2 COUNT)");
    }

    @Test
    public void testCountContributesToThreshold()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "3")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("cnt_1", BIGINT), p.rowExpression("count(col1)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
    }

    @Test
    public void testFiresForCountOnDifferentArgumentTypes()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("dcol1", DOUBLE);
                    af.globalGrouping()
                            .addAggregation(p.variable("cnt_1", BIGINT), p.rowExpression("count(col1)"))
                            .addAggregation(p.variable("cnt_2", BIGINT), p.rowExpression("count(dcol1)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("dcol1", DOUBLE)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
    }

    @Test
    public void testFiresForExpressionSumArguments()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(col1 + col2)"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(col1 - col2)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
    }

    @Test
    public void testFiresForConditionalSumWithIf()
    {
        PlanNode result = tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("flag", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1", BIGINT), p.rowExpression("sum(if(flag > bigint '0', col1))"))
                            .addAggregation(p.variable("sum_2", BIGINT), p.rowExpression("sum(if(flag > bigint '0', col2))"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("flag", BIGINT)));
                }))
                .get();

        assertVectorSumPlanStructure(result, 1);
    }

    // ---- Negative tests: rule does not fire ----

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "0")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", BIGINT)));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireBelowThreshold()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "5")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1"), p.variable("col2"), p.variable("col3")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinctSum()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"), true)
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"), true)
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForDistinctCount()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("cnt_1"), p.rowExpression("count(col1)"), true)
                            .addAggregation(p.variable("cnt_2"), p.rowExpression("count(col2)"), true)
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForCountsBelowThreshold()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "5")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("cnt_1"), p.rowExpression("count(col1)"))
                            .addAggregation(p.variable("cnt_2"), p.rowExpression("count(col2)"))
                            .addAggregation(p.variable("cnt_3"), p.rowExpression("count(col3)"))
                            .source(p.values(p.variable("col1"), p.variable("col2"), p.variable("col3")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForMixedTypesBelowThreshold()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "3")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    p.variable("col3", DOUBLE);
                    af.globalGrouping()
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .addAggregation(p.variable("sum_3"), p.rowExpression("sum(col3)"))
                            .source(p.values(p.variable("col1", BIGINT), p.variable("col2", BIGINT), p.variable("col3", DOUBLE)));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForNonSumAggregations()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .addAggregation(p.variable("max_1"), p.rowExpression("max(col1)"))
                            .addAggregation(p.variable("max_2"), p.rowExpression("max(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForPartialAggregation()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.PARTIAL)
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForIntermediateAggregation()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.INTERMEDIATE)
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForFinalAggregation()
    {
        tester().assertThat(new MergeSumsToVectorSum(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(MERGE_SUMS_TO_VECTOR_SUM_THRESHOLD, "2")
                .on(p -> p.aggregation(af -> {
                    p.variable("col1", BIGINT);
                    p.variable("col2", BIGINT);
                    af.globalGrouping()
                            .step(AggregationNode.Step.FINAL)
                            .addAggregation(p.variable("sum_1"), p.rowExpression("sum(col1)"))
                            .addAggregation(p.variable("sum_2"), p.rowExpression("sum(col2)"))
                            .source(p.values(p.variable("col1"), p.variable("col2")));
                }))
                .doesNotFire();
    }

    // ---- Helper ----

    private static void assertVectorSumPlanStructure(PlanNode result, int expectedVectorSumCount)
    {
        assertTrue(result instanceof ProjectNode,
                "Expected outer ProjectNode but got " + result.getClass().getSimpleName());
        ProjectNode outerProject = (ProjectNode) result;

        assertTrue(outerProject.getSource() instanceof AggregationNode,
                "Expected AggregationNode but got " + outerProject.getSource().getClass().getSimpleName());
        AggregationNode aggNode = (AggregationNode) outerProject.getSource();

        assertTrue(aggNode.getSource() instanceof ProjectNode,
                "Expected inner ProjectNode but got " + aggNode.getSource().getClass().getSimpleName());

        long vectorSumCount = aggNode.getAggregations().values().stream()
                .filter(agg -> agg.getCall().getDisplayName().equals("vector_sum"))
                .count();
        assertEquals(vectorSumCount, expectedVectorSumCount,
                "Expected " + expectedVectorSumCount + " vector_sum aggregation(s)");

        long elementAtCount = outerProject.getAssignments().getMap().values().stream()
                .filter(expr -> expr instanceof CallExpression
                        && ((CallExpression) expr).getDisplayName().equals("element_at"))
                .count();
        assertTrue(elementAtCount > 0, "Expected element_at projections in outer ProjectNode");
    }
}
