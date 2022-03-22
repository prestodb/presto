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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.SampleNode;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class TestSampleStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testSample()
    {
        tester().assertStatsFor(pb ->
                pb.sample(0.5, SampleNode.Type.BERNOULLI, pb.values(
                        pb.variable("i1", BIGINT), pb.variable("i2", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(new VariableReferenceExpression("i1", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(100)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i2", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(20)
                                .setDistinctValuesCount(20)
                                .setNullsFraction(0.2)
                                .build())
                        .build())
                .check(check ->
                        check.outputRowsCount(50)
                                .variableStats(new VariableReferenceExpression("i1", BIGINT), assertion -> assertion
                                        .lowValue(1)
                                        .highValue(100)
                                        .distinctValuesCount(5)
                                        .nullsFraction(0))
                                .variableStats(new VariableReferenceExpression("i2", BIGINT), assertion -> assertion
                                        .lowValue(1)
                                        .highValue(20)
                                        .distinctValuesCount(20)
                                        .nullsFraction(0.2)));

        tester().assertStatsFor(pb ->
                pb.sample(0.2, SampleNode.Type.SYSTEM, pb.values(
                        pb.variable("i1", BIGINT), pb.variable("i2", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .addVariableStatistics(new VariableReferenceExpression("i1", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(100)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i2", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(20)
                                .setDistinctValuesCount(20)
                                .setNullsFraction(0)
                                .build())
                        .build())
                .check(check ->
                        check.outputRowsCount(20)
                                .variableStats(new VariableReferenceExpression("i1", BIGINT), assertion -> assertion
                                        .lowValue(1)
                                        .highValue(100)
                                        .distinctValuesCount(5)
                                        .nullsFraction(0))
                                .variableStats(new VariableReferenceExpression("i2", BIGINT), assertion -> assertion
                                        .lowValue(1)
                                        .highValue(20)
                                        .distinctValuesCount(20)
                                        .nullsFraction(0)));
    }
}
