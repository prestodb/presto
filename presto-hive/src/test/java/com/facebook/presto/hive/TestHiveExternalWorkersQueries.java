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
import com.facebook.presto.spi.api.Experimental;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

import static com.facebook.presto.hive.HiveQueryRunner.HIVE_CATALOG;

@Experimental
public class TestHiveExternalWorkersQueries
        extends AbstractTestQueryFramework
{
    protected TestHiveExternalWorkersQueries()
    {
        super(HiveExternalWorkerQueryRunner::createQueryRunner);
    }

    @Test
    public void testFiltersAndProjections()
    {
        assertQuery("SELECT * FROM nation");
        assertQuery("SELECT * FROM nation WHERE nationkey = 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <> 4");
        assertQuery("SELECT * FROM nation WHERE nationkey < 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey > 4");
        assertQuery("SELECT * FROM nation WHERE nationkey >= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey BETWEEN 3 AND 7");
        assertQuery("SELECT nationkey * 10, nationkey % 5, -nationkey, nationkey / 3 FROM nation");
        assertQuery("SELECT *, nationkey / 3 FROM nation");

        assertQuery("SELECT rand() < 1, random() < 1 FROM nation", "SELECT true, true FROM nation");
        assertQuery("SELECT ceil(discount), ceiling(discount), floor(discount), abs(discount) FROM lineitem");
        assertQuery("SELECT substr(comment, 1, 10), length(comment) FROM orders");
    }

    @Test
    public void testFilterPushdown()
    {
        Session filterPushdown = Session.builder(getSession())
                .setCatalogSessionProperty(HIVE_CATALOG, "pushdown_filter_enabled", "true")
                .build();

        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey = 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey <> 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey < 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey <= 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey > 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey >= 4");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey BETWEEN 3 AND 7");
        assertQuery(filterPushdown, "SELECT * FROM nation WHERE nationkey IN (1, 3, 5)");

        assertQuery(filterPushdown, "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.02");
        assertQuery(filterPushdown, "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount BETWEEN 0.01 AND 0.02");

        // no row passes the filter
        assertQuery(filterPushdown, "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.2");
    }

    @Test
    public void testAggregations()
    {
        assertQuery("SELECT count(*) FROM nation");
        assertQuery("SELECT regionkey, count(*) FROM nation GROUP BY regionkey");

        assertQuery("SELECT avg(discount), avg(quantity) FROM lineitem");
        assertQuery("SELECT linenumber, avg(discount), avg(quantity) FROM lineitem GROUP BY linenumber");

        assertQuery("SELECT sum(totalprice) FROM orders");
        assertQuery("SELECT orderpriority, sum(totalprice) FROM orders GROUP BY orderpriority");

        assertQuery("SELECT custkey, min(totalprice), max(orderkey) FROM orders GROUP BY custkey");
    }

    @Test
    public void testTopN()
    {
        assertQuery("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 5");

        assertQuery("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 50");

        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax " +
                "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 10");

        assertQuery("SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax " +
                "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 2000");
    }

    @Test
    public void testCast()
    {
        // TODO Fix cast double-to-varchar and boolean-to-varchar.
        //  cast(0.0 as varchar) should return "0.0", not "0".
        //  cast(bool as varchar) should return "TRUE" or "FALSE", not "true" or "false".
        assertQuery("SELECT CAST(linenumber as TINYINT), CAST(linenumber AS SMALLINT), " +
                "CAST(linenumber AS INTEGER), CAST(linenumber AS BIGINT), CAST(quantity AS REAL), " +
                "CAST(orderkey AS DOUBLE), CAST(orderkey AS VARCHAR) FROM lineitem");
    }

    @Test
    public void testValues()
    {
        assertQuery("SELECT 1, 0.24, ceil(4.5), 'A not too short ASCII string'");
    }
}
