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
import org.testng.annotations.Test;

import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;

public abstract class AbstractTestNativeCtasQueries
        extends AbstractTestQueryFramework
{
    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createOrders(queryRunner);
        createNation(queryRunner);
    }

    @Test
    public void testCreateTableAsSelect()
    {
        assertUpdate("CREATE TABLE IF NOT EXISTS test_ctas AS SELECT name, regionkey FROM nation", "SELECT count(*) FROM nation");
        assertTableColumnNames("test_ctas", "name", "regionkey");
        assertUpdate("DROP TABLE test_ctas");
        assertCreateTableAsSelect(
                "test_select",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

        assertCreateTableAsSelect(
                "test_group",
                "SELECT orderstatus, sum(totalprice) x FROM orders GROUP BY orderstatus",
                "SELECT count(DISTINCT orderstatus) FROM orders");

        assertCreateTableAsSelect(
                "test_join",
                "SELECT count(*) x FROM lineitem JOIN orders ON lineitem.orderkey = orders.orderkey",
                "SELECT CAST(1 as BIGINT)");

        assertCreateTableAsSelect(
                "test_limit",
                "SELECT orderstatus FROM orders ORDER BY orderstatus LIMIT 10",
                "SELECT CAST(10 as BIGINT)");

        assertCreateTableAsSelect(
                "test_unicode",
                "SELECT '\u2603' unicode",
                "SELECT CAST(1 as BIGINT)");

        assertCreateTableAsSelect(
                "test_with_no_data",
                "SELECT * FROM orders WITH NO DATA",
                "SELECT * FROM orders LIMIT 0",
                "SELECT CAST(0 as BIGINT)");

//        Tests for CREATE TABLE with UNION ALL: exercises PushTableWriteThroughUnion optimizer

        assertCreateTableAsSelect(
                "test_union_all",
                "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 0 UNION ALL " +
                        "SELECT orderdate, orderkey, totalprice FROM orders WHERE orderkey % 2 = 1",
                "SELECT orderdate, orderkey, totalprice FROM orders",
                "SELECT count(*) FROM orders");

//        Setting the `redistribute_writes` system property enables the redistribution of data before writing to mitigate the performance
//        impact in case the data is skewed. The default value is `true`.
        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "true").build(),
                "test_union_all_redistributed_writes",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertCreateTableAsSelect(
                Session.builder(getSession()).setSystemProperty("redistribute_writes", "false").build(),
                "test_union_all",
                "SELECT CAST(orderdate AS DATE) orderdate, orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT CAST(orderdate AS DATE), orderkey, totalprice FROM orders UNION ALL " +
                        "SELECT DATE '2000-01-01', 1234567890, 1.23",
                "SELECT count(*) + 1 FROM orders");

        assertExplainAnalyze("EXPLAIN ANALYZE CREATE TABLE analyze_test AS SELECT orderstatus FROM orders");
        assertQuery("SELECT * from analyze_test", "SELECT orderstatus FROM orders");
        assertUpdate("DROP TABLE analyze_test");
    }
}
