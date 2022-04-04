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
import org.testng.annotations.Test;

public class TestHiveJoinQueries
        extends AbstractTestHiveQueries
{
    public TestHiveJoinQueries()
    {
        super(true);
    }

    @Test
    public void testInnerJoin()
    {
        Session partitionedJoin = partitionedJoin();

        assertQuery(partitionedJoin, "SELECT o.orderstatus, l.linenumber FROM orders o, lineitem l WHERE o.orderkey = l.orderkey");
        assertQuery(partitionedJoin, "SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey > 10000");
        assertQuery(partitionedJoin, "SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");

        Session broadcastJoin = broadcastJoin();

        assertQuery(broadcastJoin, "SELECT o.orderstatus, l.linenumber FROM orders o, lineitem l WHERE o.orderkey = l.orderkey");
        assertQuery(broadcastJoin, "SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey > 10000");
        assertQuery(broadcastJoin, "SELECT count(*) FROM orders o, lineitem l WHERE o.orderkey = l.orderkey AND o.orderkey % 2 = 1");
    }

    @Test
    public void testBucketedInnerJoin()
    {
        assertQuery("SELECT b.name, c.name FROM customer_bucketed b, customer c WHERE b.name=c.name");
    }

    @Test
    public void testSemiJoin()
    {
        Session partitionedJoin = partitionedJoin();
        Session broadcastJoin = broadcastJoin();

        assertQuery(partitionedJoin, "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
        assertQuery(broadcastJoin, "SELECT * FROM lineitem WHERE orderkey IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
    }

    @Test
    public void testAntiJoin()
    {
        Session partitionedJoin = partitionedJoin();
        Session broadcastJoin = broadcastJoin();

        assertQuery(partitionedJoin, "SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
        assertQuery(broadcastJoin, "SELECT * FROM lineitem WHERE orderkey NOT IN (SELECT orderkey FROM orders WHERE (orderkey + custkey) % 2 = 0)");
    }

    @Test
    public void testLeftJoin()
    {
        assertQuery("SELECT * FROM orders o LEFT JOIN lineitem l ON o.orderkey = l.orderkey AND l.linenumber > 5");
    }

    @Test
    public void testRightJoin()
    {
        assertQuery("SELECT * FROM nation n RIGHT JOIN region r ON n.regionkey = r.regionkey");
        assertQuery("SELECT * FROM (SELECT * FROM nation WHERE regionkey % 2 = 1) n RIGHT JOIN region r ON n.regionkey = r.regionkey");
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

    private Session partitionedJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty("join_distribution_type", "PARTITIONED")
                .build();
    }

    private Session broadcastJoin()
    {
        return Session.builder(getSession())
                .setSystemProperty("join_distribution_type", "BROADCAST")
                .build();
    }
}
