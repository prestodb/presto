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

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.RANDOMIZE_PROBE_SIDE_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestRandomShuffleProbeSide
        extends BasePlanTest
{
    private Session getSessionAlwaysEnabled()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(RANDOMIZE_PROBE_SIDE_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .build();
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT * FROM lineitem join part ON lineitem.partkey = part.partkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("leftPartKey", "rightPartKey")),
                                exchange(REMOTE_STREAMING, REPARTITION,
                                        project(
                                            tableScan("lineitem", ImmutableMap.of("leftPartKey", "partkey")))),
                                anyTree(tableScan("part", ImmutableMap.of("rightPartKey", "partkey"))))),
                false);
    }
}
