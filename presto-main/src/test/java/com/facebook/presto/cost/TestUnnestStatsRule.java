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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

public class TestUnnestStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testUnnestStatsNotPopulatedForMultiRow()
    {
        tester().assertStatsFor(
                pb -> pb.unnest(
                        pb.values(pb.variable("some_map")),
                        ImmutableList.of(pb.variable("some_map")),
                        ImmutableMap.of(pb.variable("some_map"), ImmutableList.of(pb.variable("key"), pb.variable("value"))),
                        Optional.empty()))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2)
                        .addSymbolStatistics(new Symbol("some_map"), SymbolStatsEstimate.builder().setAverageRowSize(100).build())
                        .build())
                .check(check -> check.equalTo(PlanNodeStatsEstimate.unknown()));
    }

    @Test
    public void testUnntestStatsPopulated()
    {
        tester().assertStatsFor(
                pb -> pb.unnest(
                        pb.values(pb.variable("some_map")),
                        ImmutableList.of(pb.variable("some_map")),
                        ImmutableMap.of(pb.variable("some_map"), ImmutableList.of(pb.variable("key"), pb.variable("value"))),
                        Optional.empty()))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1)
                        .addSymbolStatistics(new Symbol("some_map"), SymbolStatsEstimate.builder().setAverageRowSize(100).build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .symbolStats("some_map", assertion -> assertion.averageRowSize(100))
                        .symbolStats("key", assertion -> assertion.averageRowSize(100))
                        .symbolStats("value", assertion -> assertion.averageRowSize(100)));
    }
}
