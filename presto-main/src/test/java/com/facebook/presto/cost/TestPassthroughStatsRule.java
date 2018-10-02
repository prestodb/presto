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

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestPassthroughStatsRule
        extends BaseStatsCalculatorTest
{
    private static final String SOURCE_A_COLUMN_1 = "a_column1";
    private static final String SOURCE_A_COLUMN_2 = "a_column2";
    private static final String SOURCE_B_COLUMN_1 = "b_column1";

    private static final SymbolStatsEstimate SOURCE_A_COLUMN_1_STATS =
            symbolStatistics(0.0, 20.0, 0.1, 20);
    private static final SymbolStatsEstimate SOURCE_A_COLUMN_2_STATS =
            symbolStatistics(0.0, 40.0, 0.2, 40);
    private static final SymbolStatsEstimate SOURCE_B_COLUMN_1_STATS =
            symbolStatistics(0.0, 80.0, 0.8, 80);

    private static final PlanNodeStatsEstimate SOURCE_A_STATS = PlanNodeStatsEstimate.builder()
            .setOutputRowCount(50d)
            .addSymbolStatistics(new Symbol(SOURCE_A_COLUMN_1), SOURCE_A_COLUMN_1_STATS)
            .addSymbolStatistics(new Symbol(SOURCE_A_COLUMN_2), SOURCE_A_COLUMN_2_STATS)
            .build();

    private static final PlanNodeStatsEstimate SOURCE_B_STATS = PlanNodeStatsEstimate.builder()
            .setOutputRowCount(60d)
            .addSymbolStatistics(new Symbol(SOURCE_B_COLUMN_1), SOURCE_B_COLUMN_1_STATS)
            .build();

    private static final PlanNodeStatsEstimate RESULT_STATS = PlanNodeStatsEstimate.builder()
            .setOutputRowCount(3000d)
            .addSymbolStatistics(new Symbol(SOURCE_A_COLUMN_1), SOURCE_A_COLUMN_1_STATS)
            .addSymbolStatistics(new Symbol(SOURCE_B_COLUMN_1), SOURCE_B_COLUMN_1_STATS)
            .build();

    @Test
    public void testPassthroughStatsRule()
    {
        tester().assertStatsFor(pb -> {
            Symbol sourceAColumn1 = pb.symbol(SOURCE_A_COLUMN_1, BIGINT);
            Symbol sourceAColumn2 = pb.symbol(SOURCE_A_COLUMN_2, DOUBLE);
            Symbol sourceBColumn1 = pb.symbol(SOURCE_B_COLUMN_1, BIGINT);
            return pb
                    .join(INNER,
                            pb.values(sourceAColumn1, sourceAColumn2),
                            pb.values(sourceBColumn1),
                            ImmutableList.of(new JoinNode.EquiJoinClause(sourceAColumn1, sourceBColumn1)),
                            ImmutableList.of(sourceAColumn1, sourceBColumn1),
                            Optional.empty());
        }).withSourceStats(0, SOURCE_A_STATS)
                .withSourceStats(1, SOURCE_B_STATS)
                .check(new PassthroughStatsRule<>(JoinNode.class), stats -> stats.equalTo(RESULT_STATS));
    }

    private static SymbolStatsEstimate symbolStatistics(double low, double high, double nullsFraction, double ndv)
    {
        return SymbolStatsEstimate.builder()
                .setLowValue(low)
                .setHighValue(high)
                .setNullsFraction(nullsFraction)
                .setDistinctValuesCount(ndv)
                .build();
    }
}
