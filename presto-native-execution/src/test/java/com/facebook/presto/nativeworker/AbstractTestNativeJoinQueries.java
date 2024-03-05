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
package com.facebook.presto.nativeworker;

import com.facebook.presto.Session;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createBucketedLineitemAndOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createCustomer;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createPartitionedNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;

public abstract class AbstractTestNativeJoinQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createOrders(queryRunner);
        createBucketedLineitemAndOrders(queryRunner);
        createOrdersEx(queryRunner);
        createNation(queryRunner);
        createPartitionedNation(queryRunner);
        createRegion(queryRunner);
        createCustomer(queryRunner);
        createBucketedCustomer(queryRunner);
    }

    @Test(dataProvider = "joinTypeProvider")
    public void testInnerJoin(Session joinTypeSession)
    {
        assertQuery(joinTypeSession, "SELECT o.orderstatus, l.linenumber FROM orders o, lineitem l WHERE o.orderkey = l.orderkey");
        assertQuery(joinTypeSession, "SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey > 10000");
        assertQuery(joinTypeSession, "SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");
    }

    @Test(dataProvider = "joinTypeProvider")
    public void testBucketedInnerJoin(Session joinTypeSession)
    {
        assertQuery(joinTypeSession, "SELECT b.name, c.name FROM customer_bucketed b, customer c WHERE b.name=c.name");
        assertQuery(joinTypeSession, "SELECT b.name, c.custkey FROM customer_bucketed b, customer c " +
                "WHERE b.name=c.name AND \"$bucket\" = 7");
        assertQuery(joinTypeSession, "SELECT b.* FROM customer_bucketed b, customer c " +
                "WHERE b.name=c.name AND \"$bucket\" IN (2, 5, 8)");
        assertQuery(joinTypeSession, "SELECT * FROM customer_bucketed b, customer c " +
                "WHERE b.name=c.name AND \"$bucket\" = 5");
    }

    @Test(dataProvider = "joinTypeProvider")
    public void testSemiJoin(Session joinTypeSession)
    {
        assertQuery(joinTypeSession, "SELECT * FROM orders WHERE orderdate IN (SELECT shipdate FROM lineitem) or orderdate IN (SELECT commitdate FROM lineitem)");
        assertQuery(joinTypeSession, "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
        assertQuery(joinTypeSession, "SELECT * FROM lineitem " +
                "WHERE linenumber = 3 OR orderkey IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
    }

    @Test(dataProvider = "joinTypeProvider")
    public void testAntiJoin(Session joinTypeSession)
    {
        assertQuery(joinTypeSession, "SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
        assertQuery(joinTypeSession, "SELECT * FROM lineitem " +
                "WHERE linenumber = 3 OR orderkey NOT IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");

        assertQuery("WITH mapping AS (\n" +
                "  SELECT orderkey, custkey FROM orders GROUP BY 1, 2\n" +
                ")\n" +
                "SELECT \n" +
                "  custkey\n" +
                "FROM \n" +
                "  mapping m \n" +
                "WHERE \n" +
                "  m.custkey = 38 \n" +
                "  AND m.orderkey NOT IN (SELECT orderkey FROM lineitem)");
    }

    @Test(dataProvider = "joinTypeProvider")
    public void testLeftJoin(Session joinTypeSession)
    {
        assertQuery(joinTypeSession, "SELECT * FROM orders o LEFT JOIN lineitem l ON o.orderkey = l.orderkey AND l.linenumber > 5");
    }

    @Test
    public void testRightJoinPartitioned()
    {
        Session partitionedJoin = partitionedJoin();
        assertQuery(partitionedJoin, "SELECT * FROM nation n RIGHT JOIN region r ON n.regionkey = r.regionkey");
        assertQuery(partitionedJoin, "SELECT * FROM (SELECT * FROM nation WHERE regionkey % 2 = 1) n RIGHT JOIN region r ON n.regionkey = r.regionkey");
    }

    @Test
    public void testCrossJoin()
    {
        assertQuery("SELECT * FROM nation, region");
        assertQuery("SELECT * FROM nation n, region r WHERE n.regionkey < r.regionkey");

        assertQueryReturnsEmptyResult("SELECT l.linenumber FROM lineitem l, orders o WHERE l.orderkey = o.orderkey AND o.orderkey = 12345 AND o.totalprice > 0");

        assertQuery("SELECT l.linenumber FROM lineitem l, orders o WHERE l.orderkey = o.orderkey AND o.orderkey = 14209 AND o.totalprice > 0");

        assertQuery("SELECT * FROM nation_partitioned a, nation_partitioned b");

        assertQuery("SELECT name, (SELECT max(name) FROM region WHERE regionkey = nation.regionkey) FROM nation");
    }

    @Test
    public void testMergeJoin()
    {
        String sql = "SELECT COUNT(*) FROM lineitem_bucketed a, orders_bucketed b WHERE a.orderkey = b.orderkey AND a.ds = '2021-12-20' AND b.ds = '2021-12-20'";
        assertQuery(mergeJoin(), sql, getSession(), sql);
    }

    @Test
    public void testJoinsWithoutEquiClause()
    {
        // Test double filtered left, right, full and inner joins with right constant equality.
        String query = "SELECT count(*) FROM (SELECT * FROM lineitem WHERE orderkey %% 1024 = 0) "
                + "lineitem %s JOIN (SELECT * FROM orders WHERE orderkey %% 1024 = 0) "
                + "orders ON orders.orderkey = 1024";
        assertQuery(String.format(query, "LEFT"));
        assertQuery(String.format(query, "RIGHT"));
        assertQuery(String.format(query, "FULL"));
        assertQuery(String.format(query, "INNER"));
    }

    @DataProvider(name = "joinTypeProvider")
    public Object[][] joinTypeProvider()
    {
        return joinTypeProviderImpl();
    }

    protected Object[][] joinTypeProviderImpl()
    {
        return new Object[][] {{partitionedJoin()}, {broadcastJoin()}};
    }

    protected Session partitionedJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();
    }

    protected Session broadcastJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty("join_distribution_type", "BROADCAST")
                .build();
    }

    private Session mergeJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty("prefer_merge_join_for_sorted_inputs", "true")
                .setCatalogSessionProperty("hive", "order_based_execution_enabled", "true")
                .build();
    }
}
