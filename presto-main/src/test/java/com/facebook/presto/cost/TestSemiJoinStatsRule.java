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
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestSemiJoinStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testSemiJoinPropagatesSourceStats()
    {
        VariableStatsEstimate stats = VariableStatsEstimate.builder()
                .setLowValue(1)
                .setHighValue(10)
                .setDistinctValuesCount(5)
                .setNullsFraction(0.3)
                .build();

        tester().assertStatsFor(pb -> {
            VariableReferenceExpression a = pb.variable("a", BIGINT);
            VariableReferenceExpression b = pb.variable("b", BIGINT);
            VariableReferenceExpression c = pb.variable("c", BIGINT);
            VariableReferenceExpression semiJoinOutput = pb.variable("sjo", BOOLEAN);
            return pb
                    .semiJoin(pb.values(a, b),
                            pb.values(c),
                            a,
                            c,
                            semiJoinOutput,
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
        })
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addVariableStatistics(new VariableReferenceExpression("a", BIGINT), stats)
                        .addVariableStatistics(new VariableReferenceExpression("b", BIGINT), stats)
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addVariableStatistics(new VariableReferenceExpression("c", BIGINT), stats)
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .variableStats(new VariableReferenceExpression("a", BIGINT), assertion -> assertion.isEqualTo(stats))
                        .variableStats(new VariableReferenceExpression("b", BIGINT), assertion -> assertion.isEqualTo(stats))
                        .variableStatsUnknown("c")
                        .variableStatsUnknown("sjo"));
    }
}
