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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.sidecar.functionNamespace.FunctionDefinitionProvider;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionDefinitionProvider;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManager;
import com.facebook.presto.sidecar.functionNamespace.NativeFunctionNamespaceManagerFactory;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProvider;
import com.facebook.presto.sidecar.sessionpropertyproviders.NativeSystemSessionPropertyProviderFactory;
import com.facebook.presto.sidecar.typemanager.NativeTypeManagerFactory;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.session.WorkerSessionPropertyProvider;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.Utils.checkArgument;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createLineitem;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createNation;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrders;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createOrdersEx;
import static com.facebook.presto.nativeworker.NativeQueryRunnerUtils.createRegion;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestNativeSidecarPlugin
        extends AbstractTestQueryFramework
{
    private static final String REGEX_FUNCTION_NAMESPACE = "native.default.*";
    private static final String REGEX_SESSION_NAMESPACE = "Native Execution only.*";
    private static final long SIDECAR_HTTP_CLIENT_MAX_CONTENT_SIZE_MB = 128;

    @Override
    protected void createTables()
    {
        QueryRunner queryRunner = (QueryRunner) getExpectedQueryRunner();
        createLineitem(queryRunner);
        createNation(queryRunner);
        createOrders(queryRunner);
        createOrdersEx(queryRunner);
        createRegion(queryRunner);
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        DistributedQueryRunner queryRunner = (DistributedQueryRunner) PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .setCoordinatorSidecarEnabled(true)
                .build();
        setupNativeSidecarPlugin(queryRunner);
        return queryRunner;
    }

    @Override
    protected QueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    public static void setupNativeSidecarPlugin(QueryRunner queryRunner)
    {
        queryRunner.installCoordinatorPlugin(new NativeSidecarPlugin());
        queryRunner.loadSessionPropertyProvider(
                NativeSystemSessionPropertyProviderFactory.NAME,
                ImmutableMap.of("sidecar.http-client.max-content-length", SIDECAR_HTTP_CLIENT_MAX_CONTENT_SIZE_MB + "MB"));
        queryRunner.loadFunctionNamespaceManager(
                NativeFunctionNamespaceManagerFactory.NAME,
                "native",
                ImmutableMap.of(
                        "supported-function-languages", "CPP",
                        "function-implementation-type", "CPP",
                        "sidecar.http-client.max-content-length", SIDECAR_HTTP_CLIENT_MAX_CONTENT_SIZE_MB + "MB"));
        queryRunner.loadTypeManager(NativeTypeManagerFactory.NAME);
        queryRunner.loadPlanCheckerProviderManager("native", ImmutableMap.of());
    }

    @Test
    public void testHttpClientProperties()
    {
        WorkerSessionPropertyProvider sessionPropertyProvider = getQueryRunner().getMetadata().getSessionPropertyManager().getWorkerSessionPropertyProviders().get(NativeSystemSessionPropertyProviderFactory.NAME);
        checkArgument(sessionPropertyProvider instanceof NativeSystemSessionPropertyProvider, "Expected  NativeSystemSessionPropertyProvider but got  %s", sessionPropertyProvider);
        long sessionProviderHttpClientConfigContentSize = ((NativeSystemSessionPropertyProvider) sessionPropertyProvider).getHttpClient().getMaxContentLength();
        assertEquals(sessionProviderHttpClientConfigContentSize, new DataSize(SIDECAR_HTTP_CLIENT_MAX_CONTENT_SIZE_MB, MEGABYTE).toBytes());

        FunctionNamespaceManager<? extends SqlFunction> functionNamespaceManager = getQueryRunner().getMetadata().getFunctionAndTypeManager().getFunctionNamespaceManagers().get(NativeFunctionNamespaceManagerFactory.NAME);
        checkArgument(functionNamespaceManager instanceof NativeFunctionNamespaceManager, "Expected  NativeFunctionNamespaceManager but got  %s", functionNamespaceManager);
        FunctionDefinitionProvider functionDefinitionProvider = ((NativeFunctionNamespaceManager) functionNamespaceManager).getFunctionDefinitionProvider();
        checkArgument(functionDefinitionProvider instanceof NativeFunctionDefinitionProvider, "Expected  NativeFunctionDefinitionProvider but got %s", functionDefinitionProvider);
        long functionProviderHttpClientConfigContentSize = ((NativeFunctionDefinitionProvider) functionDefinitionProvider).getHttpClient().getMaxContentLength();
        assertEquals(functionProviderHttpClientConfigContentSize, new DataSize(SIDECAR_HTTP_CLIENT_MAX_CONTENT_SIZE_MB, MEGABYTE).toBytes());
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
                        "resetSessionProperties=[], updateType=SET SESSION, clearTransactionId=false}");
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
        assertQueryFails("SELECT IF(true, 0/0, 1)", "[\\s\\S]*/ by zero native.default.fail[\\s\\S]*");
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
        assertQuery("SELECT first_value(orderdate) OVER (PARTITION BY orderkey ORDER BY totalprice RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) FROM orders");
        assertQuery("SELECT lead(orderkey, 5) OVER (PARTITION BY custkey, orderdate ORDER BY totalprice desc ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING) FROM orders");
    }

    @Test
    public void testLambdaFunctions()
    {
        // These function signatures are only supported in the native execution engine
        assertQuerySucceeds("select array_sort(array[row('apples', 23), row('bananas', 12), row('grapes', 44)], x -> x[2])");
        assertQuerySucceeds("SELECT array_sort(quantities, x -> abs(x)) FROM orders_ex");
        assertQuerySucceeds("SELECT array_sort(quantities, (x, y) -> if (x < y, cast(1 as bigint), if (x > y, cast(-1 as bigint), cast(0 as bigint)))) FROM orders_ex");

        assertQuery("SELECT array_sort(map_keys(map_union(quantity_by_linenumber))) FROM orders_ex");
        assertQuery("SELECT filter(quantities, q -> q > 10) FROM orders_ex");
        assertQuery("SELECT all_match(shuffle(quantities), x -> (x > 500.0)) FROM orders_ex");
        assertQuery("SELECT any_match(quantities, x -> TRY(((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT TRY(none_match(quantities, x -> ((10 / x) > 2))) FROM orders_ex");
        assertQuery("SELECT reduce(array[nationkey, regionkey], 103, (s, x) -> s + x, s -> s) FROM nation");
        assertQuery("SELECT transform(array[1, 2, 3], x -> x * regionkey + nationkey) FROM nation");
        assertQueryFails(
                "SELECT array_sort(quantities, (x, y, z) -> if (x < y + z, cast(1 as bigint), if (x > y + z, cast(-1 as bigint), cast(0 as bigint)))) FROM orders_ex",
                "Failed to find matching function signature for array_sort, matching failures: \n" +
                        " Exception 1: line 1:31: Expected a lambda that takes ([12])" + Pattern.quote(" argument(s) but got 3\n") +
                        " Exception 2: line 1:31: Expected a lambda that takes ([12])" + Pattern.quote(" argument(s) but got 3\n"));
    }

    @Test
    public void testApproxPercentile()
    {
        MaterializedResult raw = computeActual("SELECT orderstatus, orderkey, totalprice FROM orders");

        Multimap<String, Long> orderKeyByStatus = ArrayListMultimap.create();
        Multimap<String, Double> totalPriceByStatus = ArrayListMultimap.create();
        for (MaterializedRow row : raw.getMaterializedRows()) {
            orderKeyByStatus.put((String) row.getField(0), ((Number) row.getField(1)).longValue());
            totalPriceByStatus.put((String) row.getField(0), (Double) row.getField(2));
        }

        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, " +
                "   approx_percentile(orderkey, 0.5), " +
                "   approx_percentile(totalprice, 0.5)," +
                "   approx_percentile(orderkey, 2, 0.5)," +
                "   approx_percentile(totalprice, 2, 0.5)\n" +
                "FROM orders\n" +
                "GROUP BY orderstatus");

        for (MaterializedRow row : actual.getMaterializedRows()) {
            String status = (String) row.getField(0);
            Long orderKey = ((Number) row.getField(1)).longValue();
            Double totalPrice = (Double) row.getField(2);
            Long orderKeyWeighted = ((Number) row.getField(3)).longValue();
            Double totalPriceWeighted = (Double) row.getField(4);

            List<Long> orderKeys = Ordering.natural().sortedCopy(orderKeyByStatus.get(status));
            List<Double> totalPrices = Ordering.natural().sortedCopy(totalPriceByStatus.get(status));

            // verify real rank of returned value is within 1% of requested rank
            assertTrue(orderKey >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKey <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(orderKeyWeighted >= orderKeys.get((int) (0.49 * orderKeys.size())));
            assertTrue(orderKeyWeighted <= orderKeys.get((int) (0.51 * orderKeys.size())));

            assertTrue(totalPrice >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPrice <= totalPrices.get((int) (0.51 * totalPrices.size())));

            assertTrue(totalPriceWeighted >= totalPrices.get((int) (0.49 * totalPrices.size())));
            assertTrue(totalPriceWeighted <= totalPrices.get((int) (0.51 * totalPrices.size())));
        }
    }

    @Test
    public void testInformationSchemaTables()
    {
        assertQuery("select lower(table_name) from information_schema.tables "
                        + "where table_name = 'lineitem' or table_name = 'LINEITEM' ");
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

    @Test
    public void testAnalyzeStats()
    {
        assertUpdate("ANALYZE region", 5);

        // Show stats returns the following stats for each column in region table:
        // column_name | data_size | distinct_values_count | nulls_fraction | row_count | low_value | high_value
        assertQuery("SHOW STATS FOR region",
                "SELECT * FROM (VALUES" +
                        "('regionkey', NULL, 5e0, 0e0, NULL, '0', '4', NULL)," +
                        "('name', 5.4e1, 5e0, 0e0, NULL, NULL, NULL, NULL)," +
                        "('comment', 3.5e2, 5e0, 0e0, NULL, NULL, NULL, NULL)," +
                        "(NULL, NULL, NULL, NULL, 5e0, NULL, NULL, NULL))");

        // Create a partitioned table and run analyze on it.
        String tmpTableName = generateRandomTableName();
        try {
            getQueryRunner().execute(String.format("CREATE TABLE %s (name VARCHAR, regionkey BIGINT," +
                    "nationkey BIGINT) WITH (partitioned_by = ARRAY['regionkey','nationkey'])", tmpTableName));
            getQueryRunner().execute(
                    String.format("INSERT INTO %s SELECT name, regionkey, nationkey FROM nation", tmpTableName));
            assertQuery(String.format("SELECT * FROM %s", tmpTableName),
                    "SELECT name, regionkey, nationkey FROM nation");
            assertUpdate(String.format("ANALYZE %s", tmpTableName), 25);
            assertQuery(String.format("SHOW STATS for %s", tmpTableName),
                    "SELECT * FROM (VALUES" +
                            "('name', 2.77e2, 1e0, 0e0, NULL, NULL, NULL, NULL)," +
                            "('regionkey', NULL, 5e0, 0e0, NULL, '0', '4', NULL)," +
                            "('nationkey', NULL, 2.5e1, 0e0, NULL, '0', '24', NULL)," +
                            "(NULL, NULL, NULL, NULL, 2.5e1, NULL, NULL, NULL))");
            assertUpdate(String.format("ANALYZE %s WITH (partitions = ARRAY[ARRAY['0','0'],ARRAY['4', '11']])", tmpTableName), 2);
            assertQuery(String.format("SHOW STATS for (SELECT * FROM %s where regionkey=4 and nationkey=11)", tmpTableName),
                    "SELECT * FROM (VALUES" +
                            "('name', 8e0, 1e0, 0e0, NULL, NULL, NULL, NULL)," +
                            "('regionkey', NULL, 1e0, 0e0, NULL, '4', '4', NULL)," +
                            "('nationkey', NULL, 1e0, 0e0, NULL, '11', '11', NULL)," +
                            "(NULL, NULL, NULL, NULL, 1e0, NULL, NULL, NULL))");
        }
        finally {
            dropTableIfExists(tmpTableName);
        }
    }

    @Test
    public void testGeometryQueries()
    {
        assertQuery("SELECT ST_DISTANCE(ST_POINT(0,  0), ST_POINT(3, 4))");
        assertQuery("SELECT ST_CONTAINS(" +
                "ST_GeometryFromText('POLYGON((0 0, 0 10, 10 10, 10 0, 0 0))'), " +
                "ST_POINT(5, 5))");
        assertQuery("SELECT ST_POINT(nationkey, regionkey) from nation");
        assertQuery("SELECT " +
                "ST_DISTANCE(ST_POINT(a.nationkey, a.regionkey), ST_POINT(b.nationkey, b.regionkey)) " +
                "FROM nation a JOIN nation b ON a.nationkey < b.nationkey");
        assertQueryFails(
                "WITH regions(name, geom) AS (VALUES" +
                        "        ('A', ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'))," +
                        "        ('B', ST_GeometryFromText('POLYGON ((5 0, 5 5, 10 5, 10 0, 5 0))')))," +
                        "points(id, geom) AS (VALUES" +
                        "        ('P1', ST_Point(1, 1))," +
                        "        ('P2', ST_Point(6, 1))," +
                        "        ('P3', ST_Point(8, 4)))" +
                        "SELECT p.id, r.name FROM points p LEFT JOIN regions r ON ST_Within(p.geom, r.geom)",
                "Error from native plan checker: .SpatialJoinNode no abstract type PlanNode ");
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
