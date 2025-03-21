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
package com.facebook.presto.sidecar;

import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ImmutableList;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.sidecar.NativeSidecarPluginQueryRunnerUtils.setupNativeSidecarPlugin;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestNativeSidecarPlugin
        extends AbstractTestQueryFramework
{
    private static final String REGEX_FUNCTION_NAMESPACE = "native.default.*";
    private static final String REGEX_SESSION_NAMESPACE = "Native Execution only.*";

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.createQueryRunner(true, true, false, false);
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.createJavaQueryRunner();
    }

    @Test
    public void testShowSession()
    {
        @Language("SQL") String sql = "SHOW SESSION";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        List<MaterializedRow> filteredRows = excludeSystemSessionProperties(actualRows);
        assertFalse(filteredRows.isEmpty());
    }

    @Test
    public void testSetJavaWorkerSessionProperty()
    {
        assertQueryFails("SET SESSION aggregation_spill_enabled=false", "line 1:1: Session property aggregation_spill_enabled does not exist");
    }

    @Test
    public void testSetNativeWorkerSessionProperty()
    {
        @Language("SQL") String setSession = "SET SESSION driver_cpu_time_slice_limit_ms=500";
        MaterializedResult setSessionResult = computeActual(setSession);
        assertEquals(
                setSessionResult.toString(),
                "MaterializedResult{rows=[[true]], " +
                        "types=[boolean], " +
                        "setSessionProperties={driver_cpu_time_slice_limit_ms=500}, " +
                        "resetSessionProperties=[], updateType=SET SESSION}");
    }

    @Test
    public void testShowFunctions()
    {
        @Language("SQL") String sql = "SHOW FUNCTIONS";
        MaterializedResult actualResult = computeActual(sql);
        List<MaterializedRow> actualRows = actualResult.getMaterializedRows();
        for (MaterializedRow actualRow : actualRows) {
            List<Object> row = actualRow.getFields();
            // No namespace should be present on the functionNames
            String functionName = row.get(0).toString();
            if (Pattern.matches(REGEX_FUNCTION_NAMESPACE, functionName)) {
                fail(format("Namespace match found for row: %s", row));
            }

            // function namespace should be present.
            String fullFunctionName = row.get(5).toString();
            if (Pattern.matches(REGEX_FUNCTION_NAMESPACE, fullFunctionName)) {
                continue;
            }
            fail(format("No namespace match found for row: %s", row));
        }
    }

    @Test
    public void testGeneralQueries()
    {
        assertQuery("SELECT ARRAY['abc']");
        assertQuery("SELECT ARRAY[1, 2, 3]");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), trim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), ltrim(comment) FROM orders");
        assertQuery("SELECT substr(comment, 1, 10), length(comment), rtrim(comment) FROM orders");
        assertQuery("select lower(comment) from nation");
        assertQuery("SELECT trim(comment, ' ns'), ltrim(comment, 'a b c'), rtrim(comment, 'l y') FROM orders");
        assertQuery("select array[nationkey], array_constructor(comment) from nation");
        assertQuery("SELECT nationkey, bit_count(nationkey, 10) FROM nation ORDER BY 1");
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK%'");
        assertQuery("SELECT * FROM lineitem WHERE shipinstruct like 'TAKE BACK#%' escape '#'");
        assertQuery("SELECT orderkey, date_trunc('year', from_unixtime(orderkey, '-03:00')), date_trunc('quarter', from_unixtime(orderkey, '+14:00')), " +
                "date_trunc('month', from_unixtime(orderkey, '+03:00')), date_trunc('day', from_unixtime(orderkey, '-07:00')), " +
                "date_trunc('hour', from_unixtime(orderkey, '-09:30')), date_trunc('minute', from_unixtime(orderkey, '+05:30')), " +
                "date_trunc('second', from_unixtime(orderkey, '+00:00')) FROM orders");
        assertQuery("SELECT mod(orderkey, linenumber) FROM lineitem");
    }

    @Test
    public void testAggregateFunctions()
    {
        assertQuery("select corr(nationkey, nationkey) from nation");
        assertQuery("select count(comment) from orders");
        assertQuery("select count(*) from nation");
        assertQuery("select count(abs(orderkey) between 1 and 60000) from orders group by orderkey");
        assertQuery("SELECT count(orderkey) FROM orders WHERE orderkey < 0 GROUP BY GROUPING SETS (())");
        // tinyint
        assertQuery("SELECT sum(cast(linenumber as tinyint)), sum(cast(linenumber as tinyint)) FROM lineitem");
        // smallint
        assertQuery("SELECT sum(cast(linenumber as smallint)), sum(cast(linenumber as smallint)) FROM lineitem");
        // integer
        assertQuery("SELECT sum(linenumber), sum(linenumber) FROM lineitem");
        // bigint
        assertQuery("SELECT sum(orderkey), sum(orderkey) FROM lineitem");
        // real
        assertQuery("SELECT sum(tax_as_real), sum(tax_as_real) FROM lineitem");
        // double
        assertQuery("SELECT sum(quantity), sum(quantity) FROM lineitem");
        // date
        assertQuery("SELECT approx_distinct(orderdate, 0.023) FROM orders");
        // timestamp
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP)) FROM orders");
        assertQuery("SELECT approx_distinct(CAST(orderdate AS TIMESTAMP), 0.023) FROM orders");
        assertQuery("SELECT checksum(from_unixtime(orderkey, '+01:00')) FROM lineitem WHERE orderkey < 20");
        assertQuerySucceeds("SELECT shuffle(array_sort(quantities)) FROM orders_ex");
        assertQuery("SELECT array_sort(shuffle(quantities)) FROM orders_ex");
        assertQuery("SELECT orderkey, array_sort(reduce_agg(linenumber, CAST(array[] as ARRAY(INTEGER)), (s, x) -> s || x, (s, s2) -> s || s2)) FROM lineitem group by orderkey");
    }

    @Test
    public void testWindowFunctions()
    {
        assertQuery("SELECT * FROM (SELECT row_number() over(partition by orderstatus order by orderkey, orderstatus) rn, * from orders) WHERE rn = 1");
        assertQuery("WITH t AS (SELECT linenumber, row_number() over (partition by linenumber order by linenumber) as rn FROM lineitem) SELECT * FROM t WHERE rn = 1");
        assertQuery("SELECT row_number() OVER (PARTITION BY orderdate ORDER BY orderdate) FROM orders");
        assertQuery("SELECT min(orderkey) OVER (PARTITION BY orderdate ORDER BY orderdate, totalprice) FROM orders");
        assertQuery("SELECT sum(rn) FROM (SELECT row_number() over() rn, * from orders) WHERE rn = 10");
        assertQuery("SELECT * FROM (SELECT row_number() over(partition by orderstatus order by orderkey) rn, * from orders) WHERE rn = 1");
    }

    @Test
    public void testArraySort()
    {
        assertQueryFails("SELECT array_sort(quantities, (x, y) -> if (x < y, 1, if (x > y, -1, 0))) FROM orders_ex",
                "line 1:31: Expected a lambda that takes 1 argument\\(s\\) but got 2");
    }

    @Test
    public void testInformationSchemaTables()
    {
        assertQueryFails("select lower(table_name) from information_schema.tables "
                        + "where table_name = 'lineitem' or table_name = 'LINEITEM' ",
                "Compiler failed");
    }

    @Test
    public void testShowStats()
    {
        String tmpTableName = generateRandomTableName();
        try {
            getQueryRunner().execute(String.format("CREATE TABLE %s (c0 DECIMAL(15,2), c1 DECIMAL(38,2)) WITH (format = 'PARQUET')", tmpTableName));
            getQueryRunner().execute(String.format("INSERT INTO %s VALUES (DECIMAL '0', DECIMAL '0'), (DECIMAL '1.2', DECIMAL '3.4'), "
                    + "(DECIMAL '1000000.12', DECIMAL '28239823232323.57'), " +
                    "(DECIMAL '-542392.89', DECIMAL '-6723982392109.29'), (NULL, NULL), "
                    + "(NULL, DECIMAL'-6723982392109.29'),(DECIMAL'1.2', NULL)", tmpTableName));
            assertQuery(String.format("SHOW STATS for %s", tmpTableName));
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    private String generateRandomTableName()
    {
        String tableName = "tmp_presto_" + UUID.randomUUID().toString().replace("-", "");
        // Clean up if the temporary named table already exists.
        dropTableIfExists(tableName);
        return tableName;
    }

    private void dropTableIfExists(String tableName)
    {
        // An ugly workaround for the lack of getExpectedQueryRunner()
        computeExpected(String.format("DROP TABLE IF EXISTS %s", tableName), ImmutableList.of(BIGINT));
    }

    private List<MaterializedRow> excludeSystemSessionProperties(List<MaterializedRow> inputRows)
    {
        return inputRows.stream()
                .filter(row -> Pattern.matches(REGEX_SESSION_NAMESPACE, row.getFields().get(4).toString()))
                .collect(Collectors.toList());
    }
}
