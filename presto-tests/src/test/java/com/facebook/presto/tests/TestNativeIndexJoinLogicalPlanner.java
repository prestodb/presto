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

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.IndexedTpchPlugin;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.indexJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.indexSource;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.AbstractTestIndexedQueries.INDEX_SPEC;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public class TestNativeIndexJoinLogicalPlanner
        extends AbstractTestQueryFramework
{
    public static final List<String> SUPPORTED_JOIN_TYPES = ImmutableList.of("INNER", "LEFT");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch_indexed")
                .setSchema(TINY_SCHEMA_NAME)
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false")
                .build();

        DistributedQueryRunner queryRunner = new DistributedQueryRunner.Builder(session)
                .setNodeCount(1)
                .build();

        queryRunner.installPlugin(new IndexedTpchPlugin(INDEX_SPEC));
        queryRunner.createCatalog("tpch_indexed", "tpch_indexed");
        return queryRunner;
    }

    @Test
    public void testBasicIndexJoin()
    {
        for (String joinType : SUPPORTED_JOIN_TYPES) {
            assertPlan("" +
                            "SELECT *\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM lineitem\n" +
                            "  WHERE partkey % 8 = 0) l\n" +
                            joinType + " JOIN orders o\n" +
                            "  ON l.orderkey = o.orderkey",
                    anyTree(indexJoin(
                            filter(tableScan("lineitem")),
                            indexSource("orders"))));

            assertPlan("" +
                            "SELECT *\n" +
                            "FROM (\n" +
                            "  SELECT CASE WHEN suppkey % 2 = 0 THEN 'F' ELSE 'O' END AS orderstatus, *\n" +
                            "  FROM lineitem\n" +
                            "  WHERE partkey % 8 = 0) l\n" +
                            joinType + " JOIN orders o\n" +
                            "  ON l.orderkey = o.orderkey\n" +
                            "  AND l.orderstatus = o.orderstatus",
                    anyTree(indexJoin(
                            project(filter(tableScan("lineitem"))),
                            indexSource("orders"))));
        }
    }

    @Test
    public void testNonEqualIndexJoin()
    {
        for (String joinType : SUPPORTED_JOIN_TYPES) {
            assertPlan("" +
                            "SELECT *\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM lineitem\n" +
                            "  WHERE partkey % 8 = 0) l\n" +
                            joinType + " JOIN orders o\n" +
                            "  ON l.orderkey = o.orderkey" +
                            "  AND o.custkey BETWEEN 1 AND l.partkey",
                    anyTree(indexJoin(
                            filter(tableScan("lineitem")),
                            indexSource("orders"))));

            assertPlan("" +
                            "SELECT *\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM lineitem\n" +
                            "  WHERE partkey % 8 = 0) l\n" +
                            joinType + " JOIN orders o\n" +
                            "  ON l.orderkey = o.orderkey" +
                            "  AND CONTAINS(ARRAY[1, l.partkey, 3], o.custkey)",
                    anyTree(indexJoin(
                            filter(tableScan("lineitem")),
                            indexSource("orders"))));

            assertPlan("" +
                            "SELECT *\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM lineitem\n" +
                            "  WHERE partkey % 8 = 0) l\n" +
                            joinType + " JOIN orders o\n" +
                            "  ON l.orderkey = o.orderkey" +
                            "  AND o.custkey BETWEEN 1 AND 100",
                    anyTree(indexJoin(
                            filter(tableScan("lineitem")),
                            filter(indexSource("orders")))));

            assertPlan("" +
                            "SELECT *\n" +
                            "FROM (\n" +
                            "  SELECT *\n" +
                            "  FROM lineitem\n" +
                            "  WHERE partkey % 8 = 0) l\n" +
                            joinType + " JOIN orders o\n" +
                            "  ON l.orderkey = o.orderkey" +
                            "  AND CONTAINS(ARRAY[1, 2, 3], o.custkey)",
                    anyTree(indexJoin(
                            filter(tableScan("lineitem")),
                            filter(indexSource("orders")))));
        }
    }
}
