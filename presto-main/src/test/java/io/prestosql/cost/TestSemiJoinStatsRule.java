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

import java.util.Optional;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

public class TestSemiJoinStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testSemiJoinPropagatesSourceStats()
    {
        SymbolStatsEstimate stats = SymbolStatsEstimate.builder()
                .setLowValue(1)
                .setHighValue(10)
                .setDistinctValuesCount(5)
                .setNullsFraction(0.3)
                .build();

        tester().assertStatsFor(pb -> {
            Symbol a = pb.symbol("a", BIGINT);
            Symbol b = pb.symbol("b", BIGINT);
            Symbol c = pb.symbol("c", BIGINT);
            Symbol semiJoinOutput = pb.symbol("sjo", BOOLEAN);
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
                        .addSymbolStatistics(new Symbol("a"), stats)
                        .addSymbolStatistics(new Symbol("b"), stats)
                        .build())
                .withSourceStats(1, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(20)
                        .addSymbolStatistics(new Symbol("c"), stats)
                        .build())
                .check(check -> check
                        .outputRowsCount(10)
                        .symbolStats("a", assertion -> assertion.isEqualTo(stats))
                        .symbolStats("b", assertion -> assertion.isEqualTo(stats))
                        .symbolStatsUnknown("c")
                        .symbolStatsUnknown("sjo"));
    }
}
