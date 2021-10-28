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
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static java.util.Collections.emptyList;

public class TestExchangeStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testExchange()
    {
        // test cases origins from TestUnionStatsRule
        // i11, i21 have separated low/high ranges and known all stats, unknown distinct values count
        // i12, i22 have overlapping low/high ranges and known all stats, unknown nulls fraction
        // i13, i23 have some unknown range stats
        // i14, i24 have the same stats

        tester().assertStatsFor(pb -> pb
                .exchange(exchangeBuilder -> exchangeBuilder
                        .addInputsSet(pb.variable("i11", BIGINT), pb.variable("i12", BIGINT), pb.variable("i13", BIGINT), pb.variable("i14", BIGINT))
                        .addInputsSet(pb.variable("i21", BIGINT), pb.variable("i22", BIGINT), pb.variable("i23", BIGINT), pb.variable("i24", BIGINT))
                        .fixedHashDistributionParitioningScheme(
                                ImmutableList.of(pb.variable("o1", BIGINT), pb.variable("o2", BIGINT), pb.variable("o3", BIGINT), pb.variable("o4", BIGINT)),
                                emptyList())
                        .addSource(pb.values(pb.variable("i11", BIGINT), pb.variable("i12", BIGINT), pb.variable("i13", BIGINT), pb.variable("i14", BIGINT)))
                        .addSource(pb.values(pb.variable("i21", BIGINT), pb.variable("i22", BIGINT), pb.variable("i23", BIGINT), pb.variable("i24", BIGINT)))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .setTotalSize(40)
                        .addVariableStatistics(new VariableReferenceExpression("i11", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0.3)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i12", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i13", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i14", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .setTotalSize(80)
                        .addVariableStatistics(new VariableReferenceExpression("i21", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(11)
                                .setHighValue(20)
                                .setNullsFraction(0.4)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i22", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(2)
                                .setHighValue(7)
                                .setDistinctValuesCount(3)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i23", BIGINT), VariableStatsEstimate.builder()
                                .setDistinctValuesCount(6)
                                .setNullsFraction(0.2)
                                .build())
                        .addVariableStatistics(new VariableReferenceExpression("i24", BIGINT), VariableStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(30)
                        .totalSize(120)
                        .variableStats(new VariableReferenceExpression("o1", BIGINT), assertion -> assertion
                                .lowValue(1)
                                .highValue(20)
                                .distinctValuesCountUnknown()
                                .nullsFraction(0.3666666))
                        .variableStats(new VariableReferenceExpression("o2", BIGINT), assertion -> assertion
                                .lowValue(0)
                                .highValue(7)
                                .distinctValuesCount(4)
                                .nullsFractionUnknown())
                        .variableStats(new VariableReferenceExpression("o3", BIGINT), assertion -> assertion
                                .lowValueUnknown()
                                .highValueUnknown()
                                .distinctValuesCount(6)
                                .nullsFraction(0.1666667))
                        .variableStats(new VariableReferenceExpression("o4", BIGINT), assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .distinctValuesCount(4)
                                .nullsFraction(0.1)));
    }

    @Test
    public void testExchangeConfidence()
    {
        // Confidence of exchange stats should be logical AND of its source nodes' confidence

        tester().assertStatsFor(pb -> pb
                .exchange(exchangeBuilder -> exchangeBuilder
                        .addInputsSet()
                        .addInputsSet()
                        .singleDistributionPartitioningScheme()
                        .addSource(pb.values())
                        .addSource(pb.values())))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .setConfident(true)
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .setConfident(true)
                        .build())
                .check(check -> check
                        .confident(true));

        tester().assertStatsFor(pb -> pb
                .exchange(exchangeBuilder -> exchangeBuilder
                        .addInputsSet()
                        .addInputsSet()
                        .singleDistributionPartitioningScheme()
                        .addSource(pb.values())
                        .addSource(pb.values())))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .setConfident(true)
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .setConfident(false)
                        .build())
                .check(check -> check
                        .confident(false));
    }
}
