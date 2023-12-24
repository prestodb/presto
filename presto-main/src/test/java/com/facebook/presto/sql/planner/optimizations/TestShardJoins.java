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
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.SHARDED_JOINS_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.unnest;

public class TestShardJoins
        extends BasePlanTest
{
    private Session getSessionAlwaysEnabled()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SHARDED_JOINS_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.PARTITIONED.name())
                .build();
    }

    private Session getBroadcastJoinSessionAlwaysEnabled()
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(SHARDED_JOINS_STRATEGY, "ALWAYS")
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "NONE")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, FeaturesConfig.JoinDistributionType.BROADCAST.name())
                .build();
    }

    @Test
    public void testJoin()
    {
        assertPlan("SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftOrderKey", expression("leftOrderKey"), "leftShard", expression("random(100)")),
                                                tableScan("lineitem", ImmutableMap.of("leftOrderKey", "orderkey")))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("rightOrderKey", expression("rightOrderKey")),
                                                    tableScan("orders", ImmutableMap.of("rightOrderKey", "orderkey"))))))),
                false);
    }

    @Test
    public void testLeftJoin()
    {
        assertPlan("SELECT * FROM lineitem LEFT JOIN orders ON lineitem.orderkey = orders.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                anyTree(
                                        project(
                                                ImmutableMap.of("leftOrderKey", expression("leftOrderKey"), "leftShard", expression("random(100)")),
                                                tableScan("lineitem", ImmutableMap.of("leftOrderKey", "orderkey")))),
                                anyTree(
                                        unnest(
                                                project(ImmutableMap.of("rightOrderKey", expression("rightOrderKey")),
                                                        tableScan("orders", ImmutableMap.of("rightOrderKey", "orderkey"))))))),
                false);
    }

    @Test
    public void testDoesNotFireForFullOuterJoin()
    {
        assertPlan("SELECT * FROM lineitem FULL OUTER JOIN orders ON lineitem.orderkey = orders.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                project(
                                        tableScan("lineitem", ImmutableMap.of("leftOrderKey", "orderkey"))),
                                exchange(
                                        project(
                                            tableScan("orders", ImmutableMap.of("rightOrderKey", "orderkey")))))),
                false);
    }

    @Test
    public void testDoesNotFireForRightOuterJoin()
    {
        assertPlan("SELECT * FROM lineitem RIGHT OUTER JOIN orders ON lineitem.orderkey = orders.orderkey",
                getSessionAlwaysEnabled(),
                anyTree(
                        join(
                                project(
                                        tableScan("lineitem", ImmutableMap.of("leftOrderKey", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("orders", ImmutableMap.of("rightOrderKey", "orderkey")))))),
                false);
    }

    @Test
    public void testDoesNotFireForBroadcastJoin()
    {
        assertPlan("SELECT * FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                getBroadcastJoinSessionAlwaysEnabled(),
                anyTree(
                        join(
                                project(
                                        tableScan("lineitem", ImmutableMap.of("leftOrderKey", "orderkey"))),
                                exchange(
                                        project(
                                                tableScan("orders", ImmutableMap.of("rightOrderKey", "orderkey")))))),
                false);
    }
}
