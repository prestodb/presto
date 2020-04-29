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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.StructuralTestUtil.mapType;

public class TestUnnestStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testUnnestStatsNotPopulatedForMultiRow()
    {
        tester().assertStatsFor(
                pb -> pb.unnest(
                        pb.values(pb.variable("some_map", mapType(VARCHAR, VARCHAR))),
                        ImmutableList.of(pb.variable("some_map", mapType(VARCHAR, VARCHAR))),
                        ImmutableMap.of(pb.variable("some_map", mapType(VARCHAR, VARCHAR)), ImmutableList.of(pb.variable("key"), pb.variable("value"))),
                        Optional.empty()))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2)
                        .addVariableStatistics(new VariableReferenceExpression("some_map", mapType(VARCHAR, VARCHAR)), VariableStatsEstimate.builder().setAverageRowSize(100).build())
                        .build())
                .check(check -> check.equalTo(PlanNodeStatsEstimate.unknown()));
    }

    @Test
    public void testUnntestStatsPopulated()
    {
        tester().assertStatsFor(
                pb -> pb.unnest(
                        pb.values(pb.variable("some_map", mapType(VARCHAR, VARCHAR))),
                        ImmutableList.of(pb.variable("some_map", mapType(VARCHAR, VARCHAR))),
                        ImmutableMap.of(pb.variable("some_map", mapType(VARCHAR, VARCHAR)), ImmutableList.of(pb.variable("key", VARCHAR), pb.variable("value", VARCHAR))),
                        Optional.empty()))
                .withSourceStats(0, PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1)
                        .addVariableStatistics(new VariableReferenceExpression("some_map", mapType(VARCHAR, VARCHAR)), VariableStatsEstimate.builder().setAverageRowSize(100).build())
                        .build())
                .check(check -> check
                        .outputRowsCount(1)
                        .variableStats(new VariableReferenceExpression("some_map", mapType(VARCHAR, VARCHAR)), assertion -> assertion.averageRowSize(100))
                        .variableStats(new VariableReferenceExpression("key", VARCHAR), assertion -> assertion.averageRowSize(100))
                        .variableStats(new VariableReferenceExpression("value", VARCHAR), assertion -> assertion.averageRowSize(100)));
    }
}
