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

import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.FACT;
import static java.lang.Double.NaN;
import static org.testng.Assert.assertEquals;

public class TestFragmentStatsProvider
{
    @Test
    public void testFragmentStatsProvider()
    {
        FragmentStatsProvider fragmentStatsProvider = new FragmentStatsProvider();
        QueryId queryId1 = new QueryId("queryid1");
        QueryId queryId2 = new QueryId("queryid2");
        PlanFragmentId planFragmentId1 = new PlanFragmentId(1);
        PlanFragmentId planFragmentId2 = new PlanFragmentId(2);
        PlanNodeStatsEstimate planNodeStatsEstimate1 = new PlanNodeStatsEstimate(NaN, 10, FACT, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown());
        PlanNodeStatsEstimate planNodeStatsEstimate2 = new PlanNodeStatsEstimate(NaN, 100, FACT, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown());

        assertEquals(fragmentStatsProvider.getStats(queryId1, planFragmentId1), PlanNodeStatsEstimate.unknown());

        fragmentStatsProvider.putStats(queryId1, planFragmentId1, planNodeStatsEstimate1);
        // queryId1, fragmentId1 stats are available, other stats are unknown
        assertEquals(fragmentStatsProvider.getStats(queryId1, planFragmentId1), planNodeStatsEstimate1);
        assertEquals(fragmentStatsProvider.getStats(queryId2, planFragmentId1), PlanNodeStatsEstimate.unknown());
        assertEquals(fragmentStatsProvider.getStats(queryId1, planFragmentId2), PlanNodeStatsEstimate.unknown());

        // queryid1, fragmentid2 stats are available
        fragmentStatsProvider.putStats(queryId1, planFragmentId2, planNodeStatsEstimate2);
        assertEquals(fragmentStatsProvider.getStats(queryId1, planFragmentId2), planNodeStatsEstimate2);

        // queryId2, fragmentId1 stats are available
        fragmentStatsProvider.putStats(queryId2, planFragmentId1, planNodeStatsEstimate1);
        assertEquals(fragmentStatsProvider.getStats(queryId2, planFragmentId1), planNodeStatsEstimate1);

        // invalidate query1, query1 stats are no longer available, query 2 stats are still available
        fragmentStatsProvider.invalidateStats(queryId1, 2);
        assertEquals(fragmentStatsProvider.getStats(queryId1, planFragmentId1), PlanNodeStatsEstimate.unknown());
        assertEquals(fragmentStatsProvider.getStats(queryId1, planFragmentId2), PlanNodeStatsEstimate.unknown());
        assertEquals(fragmentStatsProvider.getStats(queryId2, planFragmentId1), planNodeStatsEstimate1);
    }
}
