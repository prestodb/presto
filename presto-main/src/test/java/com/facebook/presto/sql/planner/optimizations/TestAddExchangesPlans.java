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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

/**
 * These are plan tests similar to what we have for other optimizers (e.g. {@link com.facebook.presto.sql.planner.TestPredicatePushdown})
 * They test that the plan for a query after the optimizer runs is as expected.
 * These are separate from {@link TestAddExchanges} because those are unit tests for
 * how layouts get chosen.
 */
public class TestAddExchangesPlans
        extends BasePlanTest
{
    @Test
    public void testRepartitionForUnionWithAnyTableScans()
    {
        assertDistributedPlan("SELECT nationkey FROM nation UNION select regionkey from region",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("region"))))))));
        assertDistributedPlan("SELECT nationkey FROM nation UNION select 1",
                anyTree(
                        aggregation(ImmutableMap.of(),
                                anyTree(
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                tableScan("nation")))),
                                        anyTree(
                                                exchange(REMOTE_STREAMING, REPARTITION,
                                                        anyTree(
                                                                values())))))));
    }

    @Test
    public void testRepartitionForUnionAllBeforeHashJoin()
    {
        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select nationkey from nation) n join region r on n.nationkey = r.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("nation")))),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));

        assertDistributedPlan("SELECT * FROM (SELECT nationkey FROM nation UNION ALL select 1) n join region r on n.nationkey = r.regionkey",
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("nationkey", "regionkey")),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("nation", ImmutableMap.of("nationkey", "nationkey")))),
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        values()))),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPARTITION,
                                                anyTree(
                                                        tableScan("region", ImmutableMap.of("regionkey", "regionkey"))))))));
    }
}
