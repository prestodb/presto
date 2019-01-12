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
package io.prestosql.cost;

import io.prestosql.sql.planner.Symbol;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static java.lang.Double.POSITIVE_INFINITY;

public class TestOutputNodeStats
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForOutputNode()
    {
        PlanNodeStatsEstimate stats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(100)
                .addSymbolStatistics(
                        new Symbol("a"),
                        SymbolStatsEstimate.builder()
                                .setNullsFraction(0.3)
                                .setLowValue(1)
                                .setHighValue(30)
                                .setDistinctValuesCount(20)
                                .build())
                .addSymbolStatistics(
                        new Symbol("b"),
                        SymbolStatsEstimate.builder()
                                .setNullsFraction(0.6)
                                .setLowValue(13.5)
                                .setHighValue(POSITIVE_INFINITY)
                                .setDistinctValuesCount(40)
                                .build())
                .build();

        tester().assertStatsFor(pb -> pb
                .output(outputBuilder -> {
                    Symbol a = pb.symbol("a", BIGINT);
                    Symbol b = pb.symbol("b", DOUBLE);
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
