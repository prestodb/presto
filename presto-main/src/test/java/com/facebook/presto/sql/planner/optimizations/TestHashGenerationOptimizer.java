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
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.SKIP_HASH_GENERATION_FOR_JOIN_WITH_TABLE_SCAN_INPUT;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.ELIMINATE_CROSS_JOINS;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestHashGenerationOptimizer
        extends BasePlanTest
{
    @Test
    public void testSkipHashGenerationForJoinWithTableScanInput()
    {
        Session enable = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(SKIP_HASH_GENERATION_FOR_JOIN_WITH_TABLE_SCAN_INPUT, "true")
                .build();
        assertPlanWithSession("select * from lineitem l join orders o on l.partkey=o.custkey",
                enable,
                false,
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("partkey", "custkey")),
                                tableScan("lineitem", ImmutableMap.of("partkey", "partkey")),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPLICATE,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey"))))))));

        Session disable = Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, ELIMINATE_CROSS_JOINS.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, BROADCAST.name())
                .setSystemProperty(SKIP_HASH_GENERATION_FOR_JOIN_WITH_TABLE_SCAN_INPUT, "false")
                .build();
        assertPlanWithSession("select * from lineitem l join orders o on l.partkey=o.custkey",
                disable,
                false,
                anyTree(
                        join(INNER, ImmutableList.of(equiJoinClause("partkey", "custkey")),
                                project(
                                        tableScan("lineitem", ImmutableMap.of("partkey", "partkey"))),
                                anyTree(
                                        exchange(REMOTE_STREAMING, REPLICATE,
                                                anyTree(
                                                        tableScan("orders", ImmutableMap.of("custkey", "custkey"))))))));
    }
}
