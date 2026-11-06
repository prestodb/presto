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
package com.facebook.presto.tests;

import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.lang.String.format;
import static java.lang.String.join;

/**
 * End-to-end checks for filters that OR together equalities on varchar columns: the results stay correct, and the
 * join above the filter builds on the side that is actually smaller.
 */
public class TestOrOfVarcharEqualitiesQueries
        extends AbstractTestQueryFramework
{
    // tiny lineitem has 60,175 rows spread evenly over the 28 (shipmode, shipinstruct) combinations; orders has 15,000.
    // One pair keeps about 2,100 lineitem rows and twelve keep about 25,800, so the smaller join input flips between them.
    private static final String[] SHIP_MODES = {"AIR", "FOB", "MAIL", "RAIL", "REG AIR", "SHIP"};
    private static final String[] SHIP_INSTRUCTIONS = {"COLLECT COD", "NONE", "TAKE BACK RETURN"};

    @Override
    protected QueryRunner createQueryRunner()
    {
        LocalQueryRunner queryRunner = new LocalQueryRunner(testSessionBuilder()
                .setCatalog("tpch")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(JOIN_REORDERING_STRATEGY, "AUTOMATIC")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "AUTOMATIC")
                .build());
        queryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        return queryRunner;
    }

    @Test
    public void testFilterResults()
    {
        for (int pairs : new int[] {1, 2, 6, 12, 18}) {
            assertQuery("SELECT count(*), sum(quantity) FROM lineitem WHERE " + shipModeAndInstructionPairs(pairs));
        }
        assertQuery("SELECT count(*) FROM lineitem WHERE shipmode = 'AIR' OR shipmode = 'RAIL' OR shipmode = 'SHIP'");
    }

    @Test
    public void testJoinResults()
    {
        for (int pairs : new int[] {1, 12}) {
            assertQuery("SELECT count(*), sum(l.quantity), sum(o.totalprice) " +
                    "FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey " +
                    "WHERE " + shipModeAndInstructionPairs(pairs));
        }
    }

    @Test
    public void testJoinBuildsOnSmallerSide()
    {
        // One pair: the filtered lineitem is smaller than orders and becomes the build side.
        assertPlan(joinQuery(1),
                anyTree(join(
                        anyTree(tableScan("orders")),
                        anyTree(tableScan("lineitem")))));

        // Twelve pairs: the filtered lineitem is larger than orders. If the OR were estimated as a single pair,
        // lineitem would still look smaller and the join would build its hash table on it.
        assertPlan(joinQuery(12),
                anyTree(join(
                        anyTree(tableScan("lineitem")),
                        anyTree(tableScan("orders")))));
    }

    private static String joinQuery(int pairs)
    {
        return "SELECT l.quantity, o.totalprice FROM orders o JOIN lineitem l ON o.orderkey = l.orderkey WHERE " + shipModeAndInstructionPairs(pairs);
    }

    private static String shipModeAndInstructionPairs(int pairs)
    {
        List<String> branches = new ArrayList<>();
        for (String mode : SHIP_MODES) {
            for (String instruction : SHIP_INSTRUCTIONS) {
                if (branches.size() < pairs) {
                    branches.add(format("(shipmode = '%s' AND shipinstruct = '%s')", mode, instruction));
                }
            }
        }
        return join(" OR ", branches);
    }
}
