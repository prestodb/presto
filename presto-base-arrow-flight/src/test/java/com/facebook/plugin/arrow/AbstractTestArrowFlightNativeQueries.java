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
package com.facebook.plugin.arrow;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public abstract class AbstractTestArrowFlightNativeQueries
    extends AbstractTestQueryFramework
{
    @Test
    public void testFiltersAndProjections1()
    {
        assertQuery("SELECT * FROM nation");
        assertQuery("SELECT * FROM nation WHERE nationkey = 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <> 4");
        assertQuery("SELECT * FROM nation WHERE nationkey < 4");
        assertQuery("SELECT * FROM nation WHERE nationkey <= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey > 4");
        assertQuery("SELECT * FROM nation WHERE nationkey >= 4");
        assertQuery("SELECT * FROM nation WHERE nationkey BETWEEN 3 AND 7");
        assertQuery("SELECT * FROM nation WHERE nationkey IN (1, 3, 5)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 3, 5)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 8, 11)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 2, 3)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (-14, 2)");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT IN (1, 2, 3, 4, 5, 10, 11, 12, 13)");
    }

    @Test
    public void testFiltersAndProjections2()
    {
        assertQuery("SELECT * FROM nation WHERE nationkey NOT BETWEEN 3 AND 7");
        assertQuery("SELECT * FROM nation WHERE nationkey NOT BETWEEN -10 AND 5");
        assertQuery("SELECT * FROM nation WHERE nationkey < 5 OR nationkey > 10");
        assertQuery("SELECT nationkey * 10, nationkey % 5, -nationkey, nationkey / 3 FROM nation");
        assertQuery("SELECT *, nationkey / 3 FROM nation");
        assertQuery("SELECT nationkey IS NULL FROM nation");
        assertQuery("SELECT * FROM nation WHERE name <> 'SAUDI ARABIA'");
        assertQuery("SELECT * FROM nation WHERE name NOT IN ('RUSSIA', 'UNITED STATES', 'CHINA')");
        assertQuery("SELECT * FROM nation WHERE name NOT IN ('aaa', 'bbb', 'ccc', 'ddd')");
        assertQuery("SELECT * FROM nation WHERE name NOT IN ('', ';', 'new country w1th $p3c1@l ch@r@c73r5')");
        assertQuery("SELECT * FROM nation WHERE name NOT BETWEEN 'A' AND 'K'"); // should produce NegatedBytesRange
        assertQuery("SELECT * FROM nation WHERE name <= 'B' OR 'G' <= name");
    }

    @Test
    public void testFiltersAndProjections3()
    {
        assertQuery("SELECT * FROM lineitem WHERE shipmode <> 'FOB'");
        assertQuery("SELECT * FROM lineitem WHERE shipmode NOT IN ('RAIL', 'AIR')");
        assertQuery("SELECT * FROM lineitem WHERE shipmode NOT IN ('', 'TRUCK', 'FOB', 'RAIL')");

        assertQuery("SELECT rand() < 1, random() < 1 FROM nation", "SELECT true, true FROM nation");

        assertQuery("SELECT * FROM lineitem");
        assertQuery("SELECT ceil(discount), ceiling(discount), floor(discount), abs(discount) FROM lineitem");
        assertQuery("SELECT linenumber IN (2, 4, 6) FROM lineitem");
        assertQuery("SELECT orderdate FROM orders WHERE cast(orderdate as DATE) IN (cast('1997-07-29' as DATE), cast('1993-03-13' as DATE)) ORDER BY orderdate LIMIT 10");

        assertQuery("SELECT * FROM orders");

        assertQuery("SELECT coalesce(linenumber, -1) FROM lineitem");

        assertQuery("SELECT * FROM lineitem WHERE linenumber = 1");
        assertQuery("SELECT * FROM lineitem WHERE linenumber > 3");
    }

    @Test
    public void testFiltersAndProjections4()
    {
        assertQuery("SELECT * FROM lineitem WHERE linenumber = 3");
        assertQuery("SELECT * FROM lineitem WHERE linenumber > 5 AND linenumber < 2");

        assertQuery("SELECT * FROM lineitem WHERE linenumber > 5");
        assertQuery("SELECT * FROM lineitem WHERE linenumber IN (1, 2)");

        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount BETWEEN 0.01 AND 0.02");

        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE discount BETWEEN 0.01 AND 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE tax < 0.02");
        assertQuery("SELECT linenumber, orderkey, discount FROM lineitem WHERE tax BETWEEN 0.02 AND 0.06");
    }

    @Test
    public void testFiltersAndProjections6()
    {
        // query with filter using like
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK%'");
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK#%' escape '#'");

        // no row passes the filter
        assertQuery(
                "SELECT linenumber, orderkey, discount FROM lineitem WHERE discount > 0.2");

        // Double and float inequality filter
        assertQuery("SELECT SUM(discount) FROM lineitem WHERE discount != 0.04");
    }

    @Test
    public void testTopN()
    {
        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 5");

        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY nationkey LIMIT 50");

        assertQueryOrdered(
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax "
                        + "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 10");

        assertQueryOrdered(
                "SELECT orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax "
                        + "FROM lineitem ORDER BY orderkey, linenumber DESC LIMIT 2000");

        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY name LIMIT 15");
        assertQueryOrdered("SELECT nationkey, regionkey FROM nation ORDER BY name DESC LIMIT 15");

        assertQuery("SELECT linenumber, NULL FROM lineitem ORDER BY 1 LIMIT 23");
    }

    @Test
    public void testCast()
    {
        assertQuery("SELECT CAST(linenumber as TINYINT), CAST(linenumber AS SMALLINT), "
                + "CAST(linenumber AS INTEGER), CAST(linenumber AS BIGINT), CAST(quantity AS REAL), "
                + "CAST(orderkey AS DOUBLE), CAST(orderkey AS VARCHAR) FROM lineitem");

        assertQuery("SELECT CAST(0.0 as VARCHAR)");

        // Cast to varchar(n).
        assertQuery("SELECT CAST(comment as VARCHAR(1)) FROM orders");
        assertQuery("SELECT CAST(comment as VARCHAR(1000)) FROM orders WHERE LENGTH(comment) < 1000");
        assertQuery("SELECT CAST(c0 AS VARCHAR(1)) FROM ( VALUES (NULL) ) t(c0)");
        assertQuery("SELECT CAST(c0 AS VARCHAR(1)) FROM ( VALUES ('') ) t(c0)");

        assertQuery("SELECT CAST(linenumber as TINYINT), CAST(linenumber AS SMALLINT), "
                + "CAST(linenumber AS INTEGER), CAST(linenumber AS BIGINT), CAST(quantity AS REAL), "
                + "CAST(orderkey AS DOUBLE), CAST(orderkey AS VARCHAR) FROM lineitem");

        // Casts to varbinary.
        assertQuery("SELECT cast(null as varbinary)");
        assertQuery("SELECT cast('' as varbinary)");

        // Ensure timestamp casts are correct.
        assertQuery("SELECT cast(cast(shipdate as varchar) as timestamp) FROM lineitem ORDER BY 1");

        // Ensure date casts are correct.
        assertQuery("SELECT cast(cast(orderdate as varchar) as date) FROM orders ORDER BY 1");

        // Cast all integer types to short decimal
        assertQuery("SELECT CAST(linenumber as DECIMAL(2, 0)) FROM lineitem");
        assertQuery("SELECT CAST(linenumber as DECIMAL(8, 4)) FROM lineitem");
        assertQuery("SELECT CAST(CAST(linenumber as INTEGER) as DECIMAL(15, 6)) FROM lineitem");
        assertQuery("SELECT CAST(nationkey as DECIMAL(18, 6)) FROM nation");

        // Cast all integer types to long decimal
        assertQuery("SELECT CAST(linenumber as DECIMAL(25, 0)) FROM lineitem");
        assertQuery("SELECT CAST(linenumber as DECIMAL(19, 4)) FROM lineitem");
        assertQuery("SELECT CAST(CAST(linenumber as INTEGER) as DECIMAL(20, 6)) FROM lineitem");
        assertQuery("SELECT CAST(nationkey as DECIMAL(22, 6)) FROM nation");
    }

    @Test
    public void testSwitch()
    {
        assertQuery("SELECT case linenumber % 10 when orderkey % 3 then orderkey + 1 when 2 then orderkey + 2 else 0 end FROM lineitem");
        assertQuery("SELECT case linenumber when 1 then 'one' when 2 then 'two' else '...' end FROM lineitem");
        assertQuery("SELECT case when linenumber = 1 then 'one' when linenumber = 2 then 'two' else '...' end FROM lineitem");
    }

    @Test
    public void testIn()
    {
        assertQuery("SELECT linenumber IN (orderkey % 7, partkey % 5, suppkey % 3) FROM lineitem");
    }

    @Test
    public void testSubqueries()
    {
        assertQuery("SELECT name FROM nation WHERE regionkey = (SELECT max(regionkey) FROM region)");

        // Subquery returns zero rows.
        assertQuery("SELECT name FROM nation WHERE regionkey = (SELECT regionkey FROM region WHERE regionkey < 0)");

        // Subquery returns more than one row.
        assertQueryFails("SELECT name FROM nation WHERE regionkey = (SELECT regionkey FROM region)", ".*Expected single row of input. Received 5 rows.*");
    }

    @Test
    public void testArithmetic()
    {
        assertQuery("SELECT mod(orderkey, linenumber) FROM lineitem");
        assertQuery("SELECT discount * 0.123 FROM lineitem");
        assertQuery("SELECT ln(totalprice) FROM orders");
        assertQuery("SELECT sqrt(totalprice) FROM orders");
        assertQuery("SELECT radians(totalprice) FROM orders");
    }

    @Test
    public void testGreatestLeast()
    {
        assertQuery("SELECT greatest(linenumber, suppkey, partkey) from lineitem");
        assertQuery("SELECT least(shipdate, commitdate) from lineitem");
    }

    @Test
    public void testSign()
    {
        assertQuery("SELECT sign(totalprice) from orders");
        assertQuery("SELECT sign(-totalprice) from orders");
        assertQuery("SELECT sign(custkey) from orders");
        assertQuery("SELECT sign(-custkey) from orders");
        assertQuery("SELECT sign(shippriority) from orders");
    }

    @Test
    public void testQueryWithColumnHandleOrdering()
    {
        assertQuery("SELECT * FROM nation WHERE (name <= 'B' OR 'G' <= name) AND (nationkey BETWEEN 1 AND 10)");
    }
}
