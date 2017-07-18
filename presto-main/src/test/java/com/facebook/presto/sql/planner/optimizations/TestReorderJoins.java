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

import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestReorderJoins
        extends BasePlanTest
{
    private static final PlanMatchPattern SUPPLIER_TABLESCAN = tableScan("supplier", ImmutableMap.of("S_SUPPKEY", "suppkey", "S_NATIONKEY", "nationkey"));
    private static final PlanMatchPattern PART_TABLESCAN = tableScan("part", ImmutableMap.of("P_PARTKEY", "partkey"));
    private static final PlanMatchPattern PARTSUPP_TABLESCAN = tableScan("partsupp", ImmutableMap.of("PS_PARTKEY", "partkey", "PS_SUPPKEY", "suppkey"));
    private static final PlanMatchPattern NATION_TABLESCAN = tableScan("nation", ImmutableMap.of("N_NATIONKEY", "nationkey", "N_REGIONKEY", "regionkey"));
    private static final PlanMatchPattern REGION_TABLESCAN = tableScan("region", ImmutableMap.of("R_REGIONKEY", "regionkey"));

    public TestReorderJoins()
    {
        super(ImmutableMap.of(SystemSessionProperties.JOIN_REORDERING_STRATEGY, "COST_BASED"));
    }

    @Test
    public void testPartialTpchQ2JoinOrder()
    {
        assertPlan(
                "SELECT * " +
                        "FROM part p, supplier s, partsupp ps, nation n, region r " +
                        "WHERE p.size = 15 AND p.type like '%BRASS' AND s.suppkey = ps.suppkey AND p.partkey = ps.partkey " +
                        "AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey AND r.name = 'EUROPE'",
                anyTree(
                        join(
                                INNER,
                                equiJoinClause("PS_SUPPKEY", "S_SUPPKEY"),
                                anyTree(
                                        join(
                                                INNER,
                                                equiJoinClause("PS_PARTKEY", "P_PARTKEY"),
                                                anyTree(PARTSUPP_TABLESCAN),
                                                anyTree(PART_TABLESCAN))),
                                anyTree(
                                        join(
                                                INNER,
                                                equiJoinClause("S_NATIONKEY", "N_NATIONKEY"),
                                                anyTree(SUPPLIER_TABLESCAN),
                                                anyTree(
                                                        join(
                                                                INNER,
                                                                equiJoinClause("N_REGIONKEY", "R_REGIONKEY"),
                                                                anyTree(NATION_TABLESCAN),
                                                                anyTree(REGION_TABLESCAN))))))));
    }
}
