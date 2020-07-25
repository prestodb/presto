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

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class TestRowNumberStatsRule
        extends BaseStatsCalculatorTest
{
    private VariableStatsEstimate xStats = VariableStatsEstimate.builder()
            .setDistinctValuesCount(5.0)
            .setNullsFraction(0)
            .build();
    private VariableStatsEstimate yStats = VariableStatsEstimate.builder()
            .setDistinctValuesCount(5.0)
            .setNullsFraction(0.5)
            .build();

    @Test
    public void testSingleGroupingKey()
    {
        // grouping on a key with 0 nulls fraction without max rows per partition limit
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("x", BIGINT)),
                        Optional.empty(),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .variableStats(new VariableReferenceExpression("x", BIGINT), assertion -> assertion.isEqualTo(xStats))
                        .variableStats(new VariableReferenceExpression("y", BIGINT), assertion -> assertion.isEqualTo(yStats))
                        .variableStats(new VariableReferenceExpression("z", BIGINT), assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(2)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on a key with 0 nulls fraction with max rows per partition limit
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("x", BIGINT)),
                        Optional.of(1),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(5)
                        .variableStats(new VariableReferenceExpression("z", BIGINT), assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(1)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on a key with non zero nulls fraction
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("y", BIGINT)),
                        Optional.empty(),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(60)
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(60)
                        .variableStats(new VariableReferenceExpression("z", BIGINT), assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(10)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // unknown input row count
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("x", BIGINT)),
                        Optional.of(1),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), yStats)
                        .build())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);
    }

    @Test
    public void testMultipleGroupingKeys()
    {
        // grouping on multiple keys with the number of estimated groups less than the row count
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("x", BIGINT), pb.variable("y", BIGINT)),
                        Optional.empty(),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(60)
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(60)
                        .variableStats(new VariableReferenceExpression("z", BIGINT), assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(2)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on multiple keys with the number of estimated groups greater than the row count
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("x", BIGINT), pb.variable("y", BIGINT)),
                        Optional.empty(),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), yStats)
                        .build())
                .check(check -> check
                        .outputRowsCount(20)
                        .variableStats(new VariableReferenceExpression("z", BIGINT), assertion -> assertion
                                .lowValue(1)
                                .distinctValuesCount(1)
                                .nullsFraction(0)
                                .averageRowSize(BIGINT.getFixedSize())));

        // grouping on multiple keys with stats for one of the keys are unknown
        tester().assertStatsFor(pb -> pb
                .rowNumber(
                        ImmutableList.of(pb.variable("x", BIGINT), pb.variable("y", BIGINT)),
                        Optional.empty(),
                        pb.variable("z", BIGINT),
                        pb.values(pb.variable("x", BIGINT), pb.variable("y", BIGINT))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addVariableStatistics(new VariableReferenceExpression("x", BIGINT), xStats)
                        .addVariableStatistics(new VariableReferenceExpression("y", BIGINT), VariableStatsEstimate.unknown())
                        .build())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);
    }
}
