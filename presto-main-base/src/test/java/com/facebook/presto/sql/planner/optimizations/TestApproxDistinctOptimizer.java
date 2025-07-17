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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anySymbol;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestApproxDistinctOptimizer
        extends BasePlanTest
{
    @Test
    public void testReplacesConditionalApproxDistinct()
    {
        assertPlan("SELECT APPROX_DISTINCT(IF(nationkey = 1, 1)) FROM nation",
                output(
                    project(
                        ImmutableMap.of("output", expression("coalesce(intermediate, 0)")),
                            aggregation(
                                    ImmutableMap.of("intermediate", functionCall("arbitrary", ImmutableList.of("partial"))),
                                    AggregationNode.Step.FINAL,
                                    anyTree(
                                            aggregation(
                                                    ImmutableMap.of("partial", functionCall("arbitrary", false, ImmutableList.of(anySymbol()))),
                                                    AggregationNode.Step.PARTIAL,
                                                            anyTree(
                                                                    tableScan("nation"))))))));
    }

    @Test
    public void testReplacesConditionalApproxDistinctGrouped()
    {
        assertPlan("SELECT APPROX_DISTINCT(IF(nationkey = nationkey, 1)) FROM nation group by nationkey",
                output(
                    project(
                        ImmutableMap.of("output", expression("coalesce(intermediate, 0)")),
                            aggregation(
                                    ImmutableMap.of("intermediate", functionCall("arbitrary", ImmutableList.of("partial"))),
                                    AggregationNode.Step.FINAL,
                                    anyTree(
                                            aggregation(
                                                    ImmutableMap.of("partial", functionCall("arbitrary", false, ImmutableList.of(anySymbol()))),
                                                    AggregationNode.Step.PARTIAL,
                                                            anyTree(
                                                                    tableScan("nation"))))))));
    }

    @Test
    public void testDontReplaceConstantApproxDistinct()
    {
        assertPlan("SELECT APPROX_DISTINCT('constant') FROM nation",
                output(
                        aggregation(
                                ImmutableMap.of("final", functionCall("approx_distinct", ImmutableList.of("partial"))),
                                AggregationNode.Step.FINAL,
                                anyTree(
                                        aggregation(
                                                ImmutableMap.of("partial", functionCall("approx_distinct", false, ImmutableList.of(anySymbol()))),
                                                AggregationNode.Step.PARTIAL,
                                                        anyTree(
                                                                tableScan("nation")))))));
    }

    @Test
    public void testDontReplaceVariableApproxDistinct()
    {
        assertPlan("SELECT APPROX_DISTINCT(nationkey) FROM nation",
                output(
                        aggregation(
                                ImmutableMap.of("final", functionCall("approx_distinct", ImmutableList.of("partial"))),
                                AggregationNode.Step.FINAL,
                                anyTree(
                                        aggregation(
                                                ImmutableMap.of("partial", functionCall("approx_distinct", ImmutableList.of("nationkey"))),
                                                AggregationNode.Step.PARTIAL,
                                                                tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))))));
    }
}
