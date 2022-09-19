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

public class TestTpchQueries
        extends AbstractTestHiveQueries
{
    public TestTpchQueries()
    {
        super(true);
    }

    @Test
    public void testMissingTpchConnector()
    {
        Session session = Session.builder(getSession())
                .setCatalog("tpch")
                .setSchema("tiny")
                .build();
        // No tpch catalog exists in the native worker.
        assertQueryFails(session, "SELECT * FROM nation", "No nodes available to run query");
    }

    @Test
    public void testTpchTinyTables()
    {
        Session session = Session.builder(getSession())
                .setCatalog("tpchstandard")
                .setSchema("tiny")
                .build();

        assertQuery(session, "SELECT count(*) FROM customer");
        assertQuery(session, "SELECT count(*) FROM lineitem");
        assertQuery(session, "SELECT count(*) FROM orders");
        assertQuery(session, "SELECT count(*) FROM nation");
        assertQuery(session, "SELECT count(*) FROM part");
        assertQuery(session, "SELECT count(*) FROM partsupp");
        assertQuery(session, "SELECT count(*) FROM region");
        assertQuery(session, "SELECT count(*) FROM supplier");
        assertQuery(session, "SELECT c_custkey, c_name, c_nationkey FROM customer");
        assertQuery(session, "SELECT n_nationkey, n_name, n_regionkey FROM nation");
        assertQuery(session, "SELECT r_regionkey, r_name FROM region");
        assertQuery(session, "SELECT SUM(l_discount) FROM lineitem WHERE l_discount != 0.04");
        assertQuery(session, "SELECT count(*), l_shipdate FROM lineitem WHERE l_linenumber = 3 GROUP BY l_shipdate");
        assertQuery(session, "SELECT ps_partkey, ps_availqty, ps_supplycost FROM partsupp WHERE ps_availqty < 5");
        assertQuery(session, "SELECT p_partkey, p_mfgr, p_brand FROM part WHERE p_size = 4 and p_partkey < 1000");
        assertQuery(session, "SELECT s_suppkey, s_phone FROM supplier WHERE s_acctbal < 100");
        assertQuery(session, "SELECT c_custkey, c_phone FROM customer WHERE c_acctbal > 9900");
        assertQuery(session, "SELECT o_orderkey, o_orderdate, o_totalprice FROM orders WHERE o_shippriority = 4");

        // No row passes the filter.
        assertQuery(session,
                "SELECT l_linenumber, l_orderkey, l_discount FROM lineitem WHERE l_discount > 0.2");
    }
}
