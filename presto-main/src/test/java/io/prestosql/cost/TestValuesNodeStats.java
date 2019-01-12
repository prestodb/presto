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

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.Symbol;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.type.UnknownType.UNKNOWN;

public class TestValuesNodeStats
        extends BaseStatsCalculatorTest
{
    @Test
    public void testStatsForValuesNode()
    {
        tester().assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT), pb.symbol("b", DOUBLE)),
                        ImmutableList.of(
                                ImmutableList.of(expression("3+3"), expression("13.5e0")),
                                ImmutableList.of(expression("55"), expression("null")),
                                ImmutableList.of(expression("6"), expression("13.5e0")))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(3)
                                .addSymbolStatistics(
                                        new Symbol("a"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0)
                                                .setLowValue(6)
                                                .setHighValue(55)
                                                .setDistinctValuesCount(2)
                                                .build())
                                .addSymbolStatistics(
                                        new Symbol("b"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0.33333333333333333)
                                                .setLowValue(13.5)
                                                .setHighValue(13.5)
                                                .setDistinctValuesCount(1)
                                                .build())
                                .build()));

        tester().assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("v", createVarcharType(30))),
                        ImmutableList.of(
                                ImmutableList.of(expression("'Alice'")),
                                ImmutableList.of(expression("'has'")),
                                ImmutableList.of(expression("'a cat'")),
                                ImmutableList.of(expression("null")))))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(4)
                                .addSymbolStatistics(
                                        new Symbol("v"),
                                        SymbolStatsEstimate.builder()
                                                .setNullsFraction(0.25)
                                                .setDistinctValuesCount(3)
                                                // TODO .setAverageRowSize(4 + 1. / 3)
                                                .build())
                                .build()));
    }

    @Test
    public void testStatsForValuesNodeWithJustNulls()
    {
        PlanNodeStatsEstimate nullAStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(1)
                .addSymbolStatistics(new Symbol("a"), SymbolStatsEstimate.zero())
                .build();

        tester().assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableList.of(
                                ImmutableList.of(expression("3 + null")))))
                .check(outputStats -> outputStats.equalTo(nullAStats));

        tester().assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableList.of(
                                ImmutableList.of(expression("null")))))
                .check(outputStats -> outputStats.equalTo(nullAStats));

        tester().assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", UNKNOWN)),
                        ImmutableList.of(
                                ImmutableList.of(expression("null")))))
                .check(outputStats -> outputStats.equalTo(nullAStats));
    }

    @Test
    public void testStatsForEmptyValues()
    {
        tester().assertStatsFor(pb -> pb
                .values(ImmutableList.of(pb.symbol("a", BIGINT)),
                        ImmutableList.of()))
                .check(outputStats -> outputStats.equalTo(
                        PlanNodeStatsEstimate.builder()
                                .setOutputRowCount(0)
                                .addSymbolStatistics(new Symbol("a"), SymbolStatsEstimate.zero())
                                .build()));
    }
}
