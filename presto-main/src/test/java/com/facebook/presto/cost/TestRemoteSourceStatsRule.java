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

import com.facebook.presto.Session;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.Double.NaN;

public class TestRemoteSourceStatsRule
        extends BaseStatsCalculatorTest
{
    @Test
    public void testRemoteSourceStatsRule()
    {
        QueryId queryId = new QueryId("testqueryid");
        Session session = testSessionBuilder()
                .setQueryId(queryId)
                .build();
        StatsCalculatorTester tester = new StatsCalculatorTester(session);

        tester.getFragmentStatsProvider().putStats(queryId, new PlanFragmentId(1), new PlanNodeStatsEstimate(NaN, 1000, true, ImmutableMap.of()));
        tester.getFragmentStatsProvider().putStats(queryId, new PlanFragmentId(2), new PlanNodeStatsEstimate(NaN, 1000, true, ImmutableMap.of()));
        tester
                .assertStatsFor(planBuilder -> planBuilder.remoteSource(ImmutableList.of(new PlanFragmentId(1), new PlanFragmentId(2))))
                .check(check -> check.totalSize(2000)
                        .outputRowsCountUnknown());
        tester.close();
    }

    @Test
    public void testRemoteSourceStatsUnknown()
    {
        tester()
                .assertStatsFor(planBuilder -> planBuilder.remoteSource(ImmutableList.of(new PlanFragmentId(1), new PlanFragmentId(2))))
                .check(check -> check.outputRowsCountUnknown()
                        .totalSizeUnknown());
    }
}
