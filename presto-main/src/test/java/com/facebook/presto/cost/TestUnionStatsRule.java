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
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestUnionStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testUnion()
    {
        // test cases
        // i11, i21 have separated low/high ranges and known all stats, unknown distinct values count
        // i12, i22 have overlapping low/high ranges and known all stats, unknown nulls fraction
        // i13, i23 have some unknown range stats
        // i14, i24 have the same stats
        // i15, i25 one has stats, other contains only nulls

        tester().assertStatsFor(pb -> pb
                .union(
                        ImmutableListMultimap.<VariableReferenceExpression, VariableReferenceExpression>builder()
                                .putAll(pb.variable("o1"), pb.variable("i11"), pb.variable("i21"))
                                .putAll(pb.variable("o2"), pb.variable("i12"), pb.variable("i22"))
                                .putAll(pb.variable("o3"), pb.variable("i13"), pb.variable("i23"))
                                .putAll(pb.variable("o4"), pb.variable("i14"), pb.variable("i24"))
                                .putAll(pb.variable("o5"), pb.variable("i15"), pb.variable("i25"))
                                .build(),
                        ImmutableList.of(
                                pb.values(pb.variable("i11"), pb.variable("i12"), pb.variable("i13"), pb.variable("i14"), pb.variable("i15")),
                                pb.values(pb.variable("i21"), pb.variable("i22"), pb.variable("i23"), pb.variable("i24"), pb.variable("i25")))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addVariableStatistics(variable("i11"), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addVariableStatistics(variable("i12"), VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(variable("i13"), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(variable("i14"), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(variable("i15"), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addVariableStatistics(variable("i21"), VariableStatsEstimate.builder()
                                .setLowValue(11)
                                .setHighValue(20)
                                .setNullsFraction(0.4)
                                .build())
                        .addVariableStatistics(variable("i22"), VariableStatsEstimate.builder()
                                .setLowValue(2)
                                .setHighValue(7)
                                .setDistinctValuesCount(3)
                                .build())
                        .addVariableStatistics(variable("i23"), VariableStatsEstimate.builder()
                                .setDistinctValuesCount(6)
                                .setNullsFraction(0.2)
                                .build())
                        .addVariableStatistics(variable("i24"), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(variable("i25"), VariableStatsEstimate.builder()
                                .setNullsFraction(1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(30)
                        .variableStats(variable("o1"), assertion -> assertion
                                .lowValue(1)
                                .highValue(20)
                                .dataSizeUnknown()
                                .nullsFraction(0.3666666))
                        .variableStats(variable("o2"), assertion -> assertion
                                .lowValue(0)
                                .highValue(7)
                                .distinctValuesCount(6.4)
                                .nullsFractionUnknown())
                        .variableStats(variable("o3"), assertion -> assertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCount(8.5)
                                .nullsFraction(0.1666667))
                        .variableStats(variable("o4"), assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .distinctValuesCount(4.0)
                                .nullsFraction(0.1))
                        .variableStats(variable("o5"), assertion -> assertion
                                .lowValue(NEGATIVE_INFINITY)
                                .highValue(POSITIVE_INFINITY)
                                .distinctValuesCountUnknown()
                                .nullsFraction(0.7)));
    }

    private VariableReferenceExpression variable(String name)
    {
        return new VariableReferenceExpression(name, BIGINT);
    }
}
