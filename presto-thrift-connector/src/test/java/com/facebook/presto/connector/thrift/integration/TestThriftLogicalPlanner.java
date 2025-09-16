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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.connector.thrift.integration.ThriftQueryRunner.createThriftQueryRunner;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.indexJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.indexSource;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

@Test(singleThreaded = true)
public class TestThriftLogicalPlanner
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createThriftQueryRunner(2, 2, true, ImmutableMap.of("native-execution-enabled", "true"));
    }

    @Test
    public void testFilterPushdown()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false")
                .build();

        assertPlan(session,
                "SELECT *\n" +
                        "FROM (\n" +
                        "  SELECT *\n" +
                        "  FROM lineitem\n" +
                        "  WHERE partkey % 8 = 0) l\n" +
                        "LEFT JOIN orders o\n" +
                        "  ON l.orderkey = o.orderkey" +
                        "  AND o.orderstatus = 'F'",
                anyTree(
                        indexJoin(
                                exchange(filter(tableScan("lineitem"))),
                                indexSource("orders"))));

        assertPlan(session,
                "SELECT *\n" +
                        "FROM lineitem l\n" +
                        "LEFT JOIN orders o\n" +
                        "  ON l.orderkey = o.orderkey" +
                        "  AND o.orderkey IS NOT NULL",
                anyTree(
                        indexJoin(
                                exchange(tableScan("lineitem")),
                                indexSource("orders"))));
    }
}
