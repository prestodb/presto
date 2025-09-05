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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static io.airlift.tpch.TpchTable.CUSTOMER;
import static io.airlift.tpch.TpchTable.ORDERS;

@Test(singleThreaded = true)
public class TestHiveBucketedTablesWithRowId
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(ORDERS, CUSTOMER),
                ImmutableMap.of(),
                Optional.empty());
    }

    @BeforeClass
    public void setUp()
    {
        // Create bucketed customer table
        assertUpdate("CREATE TABLE customer_bucketed WITH " +
                "(bucketed_by = ARRAY['custkey'], bucket_count = 13) " +
                "AS SELECT * FROM customer", 1500);

        // Create bucketed orders table
        assertUpdate("CREATE TABLE orders_bucketed WITH " +
                "(bucketed_by = ARRAY['orderkey'], bucket_count = 11) " +
                "AS SELECT * FROM orders", 15000);

        // Verify tables are created
        assertQuery("SELECT count(*) FROM customer_bucketed", "SELECT count(*) FROM customer");
        assertQuery("SELECT count(*) FROM orders_bucketed", "SELECT count(*) FROM orders");
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        try {
            assertUpdate("DROP TABLE IF EXISTS customer_bucketed");
            assertUpdate("DROP TABLE IF EXISTS orders_bucketed");
        }
        catch (Exception e) {
            // Ignore cleanup errors
        }
    }

    @Test
    public void testRowIdWithBucketColumn()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "true")
                .build();

        // Test basic query with both $row_id and $bucket
        String sql = "SELECT \"$row_id\", \"$bucket\", custkey, name " +
                "FROM customer_bucketed " +
                "WHERE \"$bucket\" = 5";

        assertPlan(session, sql, anyTree(
                project(filter(tableScan("customer_bucketed")))));

        // Test aggregation grouping by both $row_id and $bucket
        sql = "SELECT \"$row_id\", \"$bucket\", COUNT(*) " +
                "FROM customer_bucketed " +
                "GROUP BY \"$row_id\", \"$bucket\"";

        assertPlan(session, sql, anyTree(
                aggregation(ImmutableMap.of(),
                        project(tableScan("customer_bucketed")))));

        // Test join between bucketed tables using both $row_id and $bucket
        sql = "SELECT c.\"$row_id\" AS customer_row_id, " +
                "c.\"$bucket\" AS customer_bucket, " +
                "o.\"$row_id\" AS order_row_id, " +
                "o.\"$bucket\" AS order_bucket, " +
                "c.name, o.orderkey " +
                "FROM customer_bucketed c " +
                "JOIN orders_bucketed o " +
                "ON c.custkey = o.custkey " +
                "WHERE c.\"$bucket\" IN (1, 3, 5) " +
                "AND o.\"$bucket\" IN (2, 4, 6)";

        assertPlan(session, sql, anyTree(
                join(
                        project(filter(tableScan("customer_bucketed"))),
                        exchange(anyTree(tableScan("orders_bucketed"))))));
    }

    @Test
    public void testRowIdUniquePropertyWithBucketing()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "true")
                .build();

        // Test unique grouping by $row_id with bucket filtering
        String sql = "SELECT " +
                "customer_row_id, " +
                "ARBITRARY(name) AS customer_name, " +
                "ARBITRARY(bucket_num) AS customer_bucket, " +
                "ARRAY_AGG(orderkey) AS orders_info " +
                "FROM (" +
                "    SELECT " +
                "        c.\"$row_id\" AS customer_row_id, " +
                "        c.\"$bucket\" AS bucket_num, " +
                "        c.name, " +
                "        o.orderkey " +
                "    FROM customer_bucketed c " +
                "    LEFT JOIN orders_bucketed o " +
                "        ON c.custkey = o.custkey " +
                "        AND o.orderstatus IN ('O', 'F') " +
                "    WHERE c.\"$bucket\" < 5 " +
                "        AND c.nationkey IN (1, 2, 3) " +
                ") " +
                "GROUP BY customer_row_id";

        assertPlan(session, sql, anyTree(
                aggregation(ImmutableMap.of(),
                        join(
                                anyTree(tableScan("customer_bucketed")),
                                anyTree(tableScan("orders_bucketed"))))));
    }

    @Test
    public void testRowIdAndBucketInComplexQuery()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "true")
                .build();

        // Complex query with both row_id and bucket columns
        String sql = "SELECT " +
                "    unique_id, " +
                "    bucket_group, " +
                "    COUNT(*) AS order_count, " +
                "    AVG(totalprice) AS avg_price " +
                "FROM (" +
                "    SELECT " +
                "        c.\"$row_id\" AS unique_id, " +
                "        CASE " +
                "            WHEN c.\"$bucket\" < 5 THEN 'low' " +
                "            WHEN c.\"$bucket\" < 10 THEN 'medium' " +
                "            ELSE 'high' " +
                "        END AS bucket_group, " +
                "        o.totalprice " +
                "    FROM customer_bucketed c " +
                "    JOIN orders_bucketed o " +
                "        ON c.custkey = o.custkey " +
                "    WHERE o.\"$bucket\" % 2 = 0 " +
                ") t " +
                "GROUP BY unique_id, bucket_group";

        assertPlan(session, sql, anyTree(
                aggregation(ImmutableMap.of(),
                        project(project(join(
                                project(tableScan("customer_bucketed")),
                                anyTree(tableScan("orders_bucketed"))))))));
    }

    @Test
    public void testDistinctRowIdWithBucketFilter()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "true")
                .build();

        // Test DISTINCT with row_id and bucket filtering
        String sql = "SELECT " +
                "    DISTINCT c.\"$row_id\" AS unique_id, " +
                "    c.\"$bucket\" AS bucket_num, " +
                "    c.name " +
                "FROM customer_bucketed c " +
                "WHERE c.\"$bucket\" BETWEEN 3 AND 8 " +
                "    AND c.nationkey = 1";

        assertPlan(session, sql, anyTree(
                aggregation(ImmutableMap.of(),
                        project(filter(tableScan("customer_bucketed"))))));
    }

    @Test
    public void testRowIdJoinOnBucketColumn()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(UTILIZE_UNIQUE_PROPERTY_IN_QUERY_PLANNING, "true")
                .build();

        // Test joining on bucket column while selecting row_id
        String sql = "SELECT " +
                "    c.\"$row_id\" AS customer_row_id, " +
                "    o.\"$row_id\" AS order_row_id, " +
                "    c.\"$bucket\" AS shared_bucket, " +
                "    c.name, " +
                "    o.orderkey " +
                "FROM customer_bucketed c " +
                "JOIN orders_bucketed o " +
                "    ON c.\"$bucket\" = o.\"$bucket\" " +
                "WHERE c.\"$bucket\" < 5";

        assertPlan(session, sql, anyTree(
                join(
                        exchange(anyTree(tableScan("customer_bucketed"))),
                        exchange(anyTree(tableScan("orders_bucketed"))))));
    }
}
