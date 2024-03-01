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
package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestStreamingForPartialAggregation
        extends BasePlanTest
{
    TestStreamingForPartialAggregation()
    {
        super(ImmutableMap.of(STREAMING_FOR_PARTIAL_AGGREGATION_ENABLED, "true"));
    }

    @Test
    public void testMultidates()
    {
        assertPlan("SELECT clerk, count(*) FROM orders GROUP BY 1",
                anyTree(aggregation(
                        singleGroupingSet("clerk"),
                        ImmutableMap.of(Optional.empty(), functionCall("count", ImmutableList.of())),
                        ImmutableList.of("clerk"), // streaming
                        ImmutableMap.of(),
                        Optional.empty(),
                        PARTIAL,
                        tableScan("orders", ImmutableMap.of("clerk", "clerk")))));
    }
}
