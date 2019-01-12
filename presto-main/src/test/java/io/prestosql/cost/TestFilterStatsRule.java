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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static io.prestosql.testing.TestingSession.testSessionBuilder;

public class TestFilterStatsRule
        extends BaseStatsCalculatorTest
{
    public StatsCalculatorTester defaultFilterTester;

    @BeforeClass
    public void setupClass()
    {
        defaultFilterTester = new StatsCalculatorTester(
                testSessionBuilder()
                        .setSystemProperty("default_filter_factor_enabled", "true")
                        .build());
    }

    @AfterClass(alwaysRun = true)
    public void tearDownClass()
    {
        defaultFilterTester.close();
        defaultFilterTester = null;
    }

    @Test
    public void testEstimatableFilter()
    {
        tester().assertStatsFor(pb -> pb
                .filter(expression("i1 = 5"),
                        pb.values(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i3"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(2)
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(5)
                                .highValue(5)
                                .distinctValuesCount(1)
                                .dataSizeUnknown()
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(2)
                                .nullsFraction(0))
                        .symbolStats("i3", assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .dataSizeUnknown()
                                .distinctValuesCount(1.9)
                                .nullsFraction(0.05)));

        defaultFilterTester.assertStatsFor(pb -> pb
                .filter(expression("i1 = 5"),
                        pb.values(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i3"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(2)
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(5)
                                .highValue(5)
                                .distinctValuesCount(1)
                                .dataSizeUnknown()
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(2)
                                .nullsFraction(0))
                        .symbolStats("i3", assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .dataSizeUnknown()
                                .distinctValuesCount(1.9)
                                .nullsFraction(0.05)));
    }

    @Test
    public void testUnestimatableFunction()
    {
        // can't estimate function and default filter factor is turned off
        tester()
                .assertStatsFor(pb -> pb
                        .filter(expression("sin(i1) = 1"),
                                pb.values(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i3"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check.outputRowsCountUnknown());

        // can't estimate function, but default filter factor is turned on
        defaultFilterTester.assertStatsFor(pb -> pb
                .filter(expression("sin(i1) = 1"),
                        pb.values(pb.symbol("i1"), pb.symbol("i2"), pb.symbol("i3"))))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .addSymbolStatistics(new Symbol("i1"), SymbolStatsEstimate.builder()
                                .setLowValue(1)
                                .setHighValue(10)
                                .setDistinctValuesCount(5)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i2"), SymbolStatsEstimate.builder()
                                .setLowValue(0)
                                .setHighValue(3)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0)
                                .build())
                        .addSymbolStatistics(new Symbol("i3"), SymbolStatsEstimate.builder()
                                .setLowValue(10)
                                .setHighValue(15)
                                .setDistinctValuesCount(4)
                                .setNullsFraction(0.1)
                                .build())
                        .build())
                .check(check -> check
                        .outputRowsCount(9)
                        .symbolStats("i1", assertion -> assertion
                                .lowValue(1)
                                .highValue(10)
                                .dataSizeUnknown()
                                .distinctValuesCount(5)
                                .nullsFraction(0))
                        .symbolStats("i2", assertion -> assertion
                                .lowValue(0)
                                .highValue(3)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0))
                        .symbolStats("i3", assertion -> assertion
                                .lowValue(10)
                                .highValue(15)
                                .dataSizeUnknown()
                                .distinctValuesCount(4)
                                .nullsFraction(0.1)));
    }
}
