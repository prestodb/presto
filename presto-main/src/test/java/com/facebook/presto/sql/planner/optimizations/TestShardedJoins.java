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

import static com.facebook.presto.SystemSessionProperties.ELIMINATE_JOIN_SKEW_BY_SHARDING;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestShardedJoins
        extends BasePlanTest
{
    private Session getSessionAlwaysEnabled()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(ELIMINATE_JOIN_SKEW_BY_SHARDING, "ALWAYS")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .build();
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT * FROM orders JOIN lineitem ON orders.orderkey = lineitem.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("leftOrderKey", "rightOrderKey")),
                                anyTree(
                                        project(ImmutableMap.of("random", expression("random(100)")),
                                            tableScan("lineitem", ImmutableMap.of("leftOrderKey", "orderkey")))),
                                anyTree(tableScan("orders", ImmutableMap.of("rightOrderKey", "orderkey"))))),
                false);
    }
}
