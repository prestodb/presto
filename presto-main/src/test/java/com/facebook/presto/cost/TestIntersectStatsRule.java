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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class TestIntersectStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testIntersect()
    {
        // Test cases:
        // i11 and i12 have separated low and high values, all known stats
        // i21 and i22 have overlapping low and high values and unknown distinct values
        // i31 has all known statistics and i32 has all null values
        // i41 and i42 have exact same values
        tester().assertStatsFor(
                pb -> pb.intersect(
                        ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                .putAll(pb.variable("o1"), pb.variable("i11"), pb.variable("i21"))
                                .putAll(pb.variable("o2"), pb.variable("i12"), pb.variable("i22"))
                                .putAll(pb.variable("o3"), pb.variable("i13"), pb.variable("i23"))
                                .putAll(pb.variable("o4"), pb.variable("i14"), pb.variable("i24"))
                                .build(),
                        ImmutableList.of(
                                pb.values(
                                        pb.variable("i11"), pb.variable("i12"),
                                        pb.variable("i13"), pb.variable("i14")),
                                pb.values(
                                        pb.variable("i21"), pb.variable("i22"),
                                        pb.variable("i23"), pb.variable("i24"))))
        ).withSourceStats(0, PlanNodeStatsEstimate.builder()
                .setOutputRowCount(10)
                .addVariableStatistics(
                        new VariableReferenceExpression("i11", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression("i12", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression("i13", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression("i14", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(15)
                                .setHighValue(25)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build()).build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                .setOutputRowCount(5)
                .addVariableStatistics(
                        new VariableReferenceExpression("i21", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(10)
                                .setNullsFraction(0)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression("i22", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(5)
                                .setHighValue(10)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0.4)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression("i23", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(7)
                                .setHighValue(15)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression("i24", BIGINT),
                        VariableStatsEstimate.builder()
                                .setLowValue(20)
                                .setHighValue(25)
                                .setDistinctValuesCount(3)
                                .setNullsFraction(0)
                                .build()).build())
                .check(check -> check
                .outputRowsCount(1.875)
                .variableStats(
                        new VariableReferenceExpression("o1", BIGINT),
                        assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .distinctValuesCount(1.875)
                                .nullsFraction(0))
                .variableStats(
                        new VariableReferenceExpression("o2", BIGINT),
                        assertion -> assertion
                                .lowValue(5)
                                .highValue(10)
                                .distinctValuesCount(0.9375)
                                .nullsFraction(0.5))
                .variableStats(
                        new VariableReferenceExpression("o3", BIGINT),
                        assertion -> assertion
                                .lowValue(7)
                                .highValue(10)
                                .distinctValuesCount(1.875)
                                .nullsFraction(0))
                .variableStats(
                        new VariableReferenceExpression("o4", BIGINT),
                        assertion -> assertion
                                .lowValue(20)
                                .highValue(25)
                                .distinctValuesCount(1.875)
                                .nullsFraction(0)));
    }
}
