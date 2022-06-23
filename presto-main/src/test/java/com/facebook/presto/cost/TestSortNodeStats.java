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
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestSortNodeStats
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForSortNode()
    {
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(100)
                .addVariableStatistics(
                        new VariableReferenceExpression(Optional.empty(), "a", BIGINT),
                        VariableStatsEstimate.builder()
                                .setNullsFraction(0.3)
                                .setLowValue(1)
                                .setHighValue(30)
                                .setDistinctValuesCount(20)
                                .build())
                .addVariableStatistics(
                        new VariableReferenceExpression(Optional.empty(), "b", DOUBLE),
                        VariableStatsEstimate.builder()
                                .setNullsFraction(0.6)
                                .setLowValue(13.5)
                                .setHighValue(POSITIVE_INFINITY)
                                .setDistinctValuesCount(40)
                                .build())
                .build();

        tester().assertStatsFor(pb -> pb
                .output(outputBuilder -> {
                    VariableReferenceExpression a = pb.variable("a", BIGINT);
                    VariableReferenceExpression b = pb.variable("b", DOUBLE);
                    outputBuilder
                            .source(pb.values(a, b))
                            .column(a, "a1")
                            .column(a, "a2")
                            .column(b, "b");
                }))
                .withSourceStats(stats)
                .check(outputStats -> outputStats.equalTo(stats));
    }
}
