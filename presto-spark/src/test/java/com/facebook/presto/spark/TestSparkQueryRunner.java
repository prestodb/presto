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
package com.facebook.presto.spark;

import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.Test;

public class TestSparkQueryRunner
        extends AbstractTestQueryFramework
{
    public TestSparkQueryRunner()
    {
        super(() -> new SparkQueryRunner(4));
    }

    @Test
    public void testTableWrite()
    {
        assertUpdate(
                "CREATE TABLE hive.hive_test.hive_orders AS " +
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                "FROM orders",
                15000);

        assertQuery(
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM hive.hive_test.hive_orders",
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment " +
                        "FROM orders");
    }

    @Test
    public void testAggregation()
    {
        assertQuery("select partkey, count(*) c from lineitem where partkey % 10 = 1 group by partkey having count(*) = 42");
    }

    @Test
    public void testJoin()
    {
        assertQuery("SELECT l.orderkey, l.linenumber, o.orderstatus " +
                "FROM lineitem l " +
                "JOIN orders o " +
                "ON l.orderkey = o.orderkey " +
                "WHERE l.orderkey % 223 = 42 AND l.linenumber = 4 and o.orderstatus = 'O'");
    }
}
