package com.facebook.presto.nativetests;

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

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_PERCENTAGE;
import static com.facebook.presto.SystemSessionProperties.LEGACY_UNNEST;
import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public abstract class AbstractTestQueriesNative
        extends AbstractTestQueries
{
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";
    private static final String p4HlltimestampTzTypeUnsupportedError = ".*Failed to parse type \\[P4HyperLogLog]. Type not registered.*";
    private static final String applyNotRegisteredError = ".*Scalar function name not registered: presto.default.apply.*";
    private static final String failNotRegisteredError = ".*Scalar function name not registered: presto.default.fail.*";
    private static final String kp4HlltimestampTzTypeUnsupportedError = ".*inferredType Failed to parse type \\[KHyperLogLog]. Type not registered.*";
    private static final String chartimestampTzTypeUnsupportedError = ".*Failed to parse type \\[char\\(.*\\)]. syntax error, unexpected LPAREN, expecting WORD.*";
    private static final String timestampTzTypeUnsupportedError = ".*Timestamp with Timezone type is not supported in Prestissimo.*";
    private static final String hashGenerationUnsupportedError = ".*Scalar function name not registered: presto.default.\\$operator\\$hash_code.*";
    private static final String arrayComparisonUnsupportedError = ".*ARRAY comparison not supported for values that contain nulls.*";

    @Override
    @DataProvider(name = "optimize_hash_generation")
    public Object[][] optimizeHashGeneration()
    {
        return new Object[][] {{"true"}};
    }

    @DataProvider(name = "use_default_literal_coalesce")
    public static Object[][] useDefaultLiteralCoalesce()
    {
        return new Object[][] {{true}};
    }

    @Override
    @Test
    public void testCustomAdd()
    {
        assertQueryFails(
                "SELECT custom_add(orderkey, custkey) FROM orders",
                " Scalar function name not registered: presto.default.custom_add, called with arguments: \\(BIGINT, BIGINT\\).");
    }

    @Override
    @Test
    public void testCustomSum()
    {
        @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
        assertQueryFails(sql, " Aggregate function not registered: presto.default.custom_sum");
    }

    @Override
    @Test
    public void testCustomRank()
    {
        @Language("SQL") String sql = "" +
                "SELECT orderstatus, clerk, sales\n" +
                ", custom_rank() OVER (PARTITION BY orderstatus ORDER BY sales DESC) rnk\n" +
                "FROM (\n" +
                "  SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                "  FROM orders\n" +
                "  GROUP BY orderstatus, clerk\n" +
                ")\n" +
                "ORDER BY orderstatus, clerk";

        assertQueryFails(sql, " Window function not registered: presto.default.custom_rank");
    }

    @Override
    @Test
    public void testApproxMostFrequentWithLong()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertEquals(actual1.getRowCount(), 1);
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of(1L, 3L, 2L, 2L, 3L, 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, cast(x as bigint), 15) FROM (values 1, 2, 1, 3, 1, 2, 3, 4, 5) t(x)");
        assertEquals(actual2.getRowCount(), 1);
        assertEquals(actual2.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of(1L, 3L, 3L, 2L));
    }

    @Override
    @Test
    public void testGroupByLimit()
    {
        Session prefilter = Session.builder(getSession())
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, "true")
                .build();
        MaterializedResult result1 = computeActual(prefilter, "select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        MaterializedResult result2 = computeActual("select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        assertEqualsIgnoreOrder(result1, result2, "Prefilter and without prefilter don't give matching results");

        assertQueryFails(prefilter, "select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 100", hashGenerationUnsupportedError, true);
        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 4)", "select 4");
        assertQuery(prefilter, "select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders where orderkey < 50) group by orderstatus limit 100", "values (5, 'F'), (10, 'O')");

        assertQuery(prefilter, "select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders where orderkey < 50) group by orderstatus having count(1) > 1 limit 100", "values (5, 'F'), (10, 'O')");

        prefilter = Session.builder(getSession())
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT, "true")
                .setSystemProperty(PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS, "1")
                .build();

        result1 = computeActual(prefilter, "select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        result2 = computeActual("select count(shipdate), orderkey from lineitem group by orderkey limit 100000");
        assertEqualsIgnoreOrder(result1, result2, "Prefilter and without prefilter don't give matching results");

        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders group by orderkey limit 100000)", "values 15000");
        assertQuery(prefilter, "select count(1) from (select count(custkey), orderkey from orders group by orderkey limit 4)", "select 4");
        assertQuery(prefilter, "select count(1) from (select count(comment), orderstatus from (select upper(comment) comment, upper(orderstatus) orderstatus from orders) group by orderstatus limit 100000)", "values 3");
    }

    @Override
    @Test
    public void testValuesWithTimestamp()
    {
        assertQueryFails("VALUES (current_timestamp, now())", timestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testTryLambdaRepeated()
    {
        assertQueryFails("SELECT x + x FROM (SELECT apply(a, i -> i * i) x FROM (VALUES 3) t(a))", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(a, i -> i * i) + apply(a, i -> i * i) FROM (VALUES 3) t(a)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(a, i -> i * i), apply(a, i -> i * i) FROM (VALUES 3) t(a)", applyNotRegisteredError, true);
        assertQuery("SELECT try(10 / a) + try(10 / a) FROM (VALUES 5) t(a)", "SELECT 4");
        assertQuery("SELECT try(10 / a), try(10 / a) FROM (VALUES 5) t(a)", "SELECT 2, 2");
    }

    @Override
    @Test
    public void testLambdaCapture()
    {
        // Test for lambda expression without capture can be found in TestLambdaExpression

        assertQueryFails("SELECT apply(0, x -> x + c1) FROM (VALUES 1) t(c1)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(0, x -> x + t.c1) FROM (VALUES 1) t(c1)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(c1 + 10, x -> apply(x + 100, y -> t.c1)) FROM (VALUES 1) t(c1)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) u(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) r(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) u(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 'a') r(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), z -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", applyNotRegisteredError, true);

        // reference lambda variable of the not-immediately-enclosing lambda
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)", applyNotRegisteredError, true);

        // in join post-filter
        assertQueryFails("SELECT * FROM (VALUES true) t(x) left JOIN (VALUES 1001) t2(y) ON (apply(false, z -> apply(false, y -> x)))", applyNotRegisteredError, true);
    }

    @Override
    @Test(enabled = false)
    public void testArrayCumSum()
    {
        // int
        String sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, 0]), (ARRAY[]), (CAST(NULL AS array(integer)))) t(k)";
        assertQuery(sql, "values array[cast(5 as integer), cast(11 as integer), cast(11 as integer)], array[], null");

        sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, 0]), (ARRAY[]), (CAST(NULL AS array(integer))), (ARRAY [cast(2147483647 as INTEGER), 2147483647, 2147483647])) t(k)";
        assertQueryFails(sql, ".*integer overflow:.*", true);

        sql = "select array_cum_sum(k) from (values (array[cast(5 as INTEGER), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(5 as integer), cast(11 as integer), cast(null as integer), cast(null as integer), cast(null as integer)]");

        sql = "select array_cum_sum(k) from (values (array[cast(null as INTEGER), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(null as integer), cast(null as integer), cast(null as integer), cast(null as integer), cast(null as integer)]");

        // bigint
        sql = "select array_cum_sum(k) from (values (array[cast(5 as bigint), 6, 0]), (ARRAY[]), (CAST(NULL AS array(bigint))), (ARRAY [cast(2147483647 as bigint), 2147483647, 2147483647])) t(k)";
        assertQuery(sql, "values array[cast(5 as bigint), cast(11 as bigint), cast(11 as bigint)], array[], null, array[cast(2147483647 as bigint), cast(4294967294 as bigint), cast(6442450941 as bigint)]");

        sql = "select array_cum_sum(k) from (values (array[cast(5 as bigint), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(5 as bigint), cast(11 as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint)]");

        sql = "select array_cum_sum(k) from (values (array[cast(null as bigint), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(null as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint), cast(null as bigint)]");

        // real
        sql = "select array_cum_sum(k) from (values (array[cast(null as real), 6, null, 2, 3])) t(k)";
        assertQuery(sql, "values array[cast(null as real), cast(null as real), cast(null as real), cast(null as real), cast(null as real)]");

        MaterializedResult raw = computeActual("SELECT array_cum_sum(k) FROM (values (ARRAY [cast(5.1 as real), 6.1, 0.5]), (ARRAY[]), (CAST(NULL AS array(real))), " +
                "(ARRAY [cast(null as real), 6.1, 0.5]), (ARRAY [cast(2.5 as real), 6.1, null, 3.2])) t(k)");
        List<MaterializedRow> rowList = raw.getMaterializedRows();
        List<Float> actualFloat = (List<Float>) rowList.get(0).getField(0);
        List<Float> expectedFloat = ImmutableList.of(5.1f, 11.2f, 11.7f);
        // TODO: Investigate error 'java.lang.ClassCastException: java.lang.Double cannot be cast to java.lang.Float'
        for (int i = 0; i < actualFloat.size(); ++i) {
            assertTrue(actualFloat.get(i) > expectedFloat.get(i) - 1e-5 && actualFloat.get(i) < expectedFloat.get(i) + 1e-5);
        }

        actualFloat = (List<Float>) rowList.get(1).getField(0);
        assertTrue(actualFloat.isEmpty());

        actualFloat = (List<Float>) rowList.get(2).getField(0);
        assertNull(actualFloat);

        actualFloat = (List<Float>) rowList.get(3).getField(0);
        for (int i = 0; i < actualFloat.size(); ++i) {
            assertNull(actualFloat.get(i));
        }

        actualFloat = (List<Float>) rowList.get(4).getField(0);
        expectedFloat = Arrays.asList(2.5f, 8.6f, null, null);
        for (int i = 0; i < 2; ++i) {
            assertTrue(actualFloat.get(i) > expectedFloat.get(i) - 1e-5f && actualFloat.get(i) < expectedFloat.get(i) + 1e-5f);
        }
        for (int i = 2; i < actualFloat.size(); ++i) {
            assertNull(actualFloat.get(i));
        }

        // double
        raw = computeActual("SELECT array_cum_sum(k) FROM (values (ARRAY [cast(5.1 as double), 6.1, 0.5]), (ARRAY[]), (CAST(NULL AS array(double))), " +
                "(ARRAY [cast(null as double), 6.1, 0.5]), (ARRAY [cast(5.1 as double), 6.1, null, 3.2])) t(k)");
        rowList = raw.getMaterializedRows();
        List<Double> actualDouble = (List<Double>) rowList.get(0).getField(0);
        List<Double> expectedDouble = ImmutableList.of(5.1, 11.2, 11.7);
        for (int i = 0; i < actualDouble.size(); ++i) {
            assertTrue(actualDouble.get(i) > expectedDouble.get(i) - 1e-5f && actualDouble.get(i) < expectedDouble.get(i) + 1e-5f);
        }

        actualDouble = (List<Double>) rowList.get(1).getField(0);
        assertTrue(actualDouble.isEmpty());

        actualDouble = (List<Double>) rowList.get(2).getField(0);
        assertNull(actualDouble);

        actualDouble = (List<Double>) rowList.get(3).getField(0);
        for (int i = 0; i < actualDouble.size(); ++i) {
            assertNull(actualDouble.get(i));
        }

        actualDouble = (List<Double>) rowList.get(4).getField(0);
        expectedDouble = Arrays.asList(5.1, 11.2, null, null);
        for (int i = 0; i < 2; ++i) {
            assertTrue(actualDouble.get(i) > expectedDouble.get(i) - 1e-5f && actualDouble.get(i) < expectedDouble.get(i) + 1e-5f);
        }
        for (int i = 2; i < actualDouble.size(); ++i) {
            assertNull(actualDouble.get(i));
        }

        // decimal
        sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, 0]), (ARRAY[]), (CAST(NULL AS array(decimal)))) t(k)";
        assertQuery(sql, "values array[cast(5.1 as decimal), cast(11.1 as decimal), cast(11.1 as decimal)], array[], null");

        sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, null, 3]), (array[cast(null as decimal(38, 1)), 6, null, 3])) t(k)";
        assertQuery(sql, "values array[cast(5.1 as decimal), cast(11.1 as decimal), cast(null as decimal), cast(null as decimal)], " +
                "array[cast(null as decimal), cast(null as decimal), cast(null as decimal), cast(null as decimal)]");

        // varchar
        sql = "select array_cum_sum(k) from (values (array[cast('5.1' as varchar), '6', '0']), (ARRAY[]), (CAST(NULL AS array(varchar)))) t(k)";
        assertQueryFails(sql, ".*cannot be applied to.*");

        sql = "select array_cum_sum(k) from (values (array[cast(null as varchar), '6', '0'])) t(k)";
        assertQueryFails(sql, ".*cannot be applied to.*");
    }

    @Override
    @Test
    public void testLambdaInAggregationContext()
    {
        assertQueryFails("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", applyNotRegisteredError, true);
        assertQueryFails("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y", applyNotRegisteredError, true);

        assertQueryFails("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)", applyNotRegisteredError, true);
        // nested lambda expression uses the same variable name
        assertQueryFails("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)", applyNotRegisteredError, true);
    }

    @Override
    @Test
    public void testLambdaInSubqueryContext()
    {
        assertQueryFails("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply((SELECT 10), i -> i * i)", applyNotRegisteredError, true);

        // with capture
        assertQueryFails("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)", applyNotRegisteredError, true);
        assertQueryFails("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)", applyNotRegisteredError, true);
    }

    @Override
    @Test
    public void testNonDeterministic()
    {
        MaterializedResult materializedResult = computeActual("SELECT rand() FROM orders LIMIT 10");
        long distinctCount = materializedResult.getMaterializedRows().stream()
                .map(row -> row.getField(0))
                .distinct()
                .count();
        assertTrue(distinctCount >= 8, "rand() must produce different rows");

        assertQueryFails("SELECT apply(1, x -> x + rand()) FROM orders LIMIT 10", applyNotRegisteredError, true);
    }

    @Override
    @Test
    public void testMergeHyperLogLog()
    {
        assertQueryFails("SELECT cardinality(merge(create_hll(custkey))) FROM orders", "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        assertQueryFails("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders",
                "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus",
                "Scalar function name not registered: presto.default.create_hll, called with arguments", true);
    }

    @Override
    @Test
    public void testKeyBasedSampling()
    {
        String[] queries = new String[]{
                "select count(1) from orders join lineitem using(orderkey)",
                "select count(1) from (select custkey, max(orderkey) from orders group by custkey)",
                "select count_if(m >= 1) from (select max(orderkey) over(partition by custkey) m from orders)",
                "select cast(m as bigint) from (select sum(totalprice) over(partition by custkey order by comment) m from orders order by 1 desc limit 1)",
                "select count(1) from lineitem where orderkey in (select orderkey from orders where length(comment) > 7)",
                "select count(1) from lineitem where orderkey not in (select orderkey from orders where length(comment) > 27)",
                "select count(1) from (select distinct orderkey, custkey from orders)",
        };
        int[] unsampledResults = new int[]{60175, 1000, 15000, 5408941, 60175, 9256, 15000};
        for (int i = 0; i < queries.length; i++) {
            assertQuery(queries[i], "select " + unsampledResults[i]);
        }

        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, "0.2")
                .build();
        for (int i = 0; i < queries.length; i++) {
            assertQueryFails(sessionWithKeyBasedSampling, queries[i], "Scalar function name not registered: presto.default.key_sampling_percent, called with arguments", true);
        }

        sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_PERCENTAGE, "0.1")
                .build();
        for (int i = 0; i < queries.length; i++) {
            assertQueryFails(sessionWithKeyBasedSampling, queries[i], "Scalar function name not registered: presto.default.key_sampling_percent, called with arguments", true);
        }
    }

    @Override
    @Test
    public void testP4ApproxSetBigint()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) FROM orders",
                p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetVarchar()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) FROM orders",
                p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetBigintGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetDouble()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) FROM orders",
                p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetDoubleGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetGroupByWithNulls()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(custkey % 2 <> 0, custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(orderstatus != 'O', custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetOnlyNulls()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(null) AS P4HYPERLOGLOG)) FROM orders",
                p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetVarcharGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testP4ApproxSetWithNulls()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(IF(orderstatus = 'O', custkey)) AS P4HYPERLOGLOG)) FROM orders",
                p4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    public void testLargeBytecode()
    {
        StringBuilder stringBuilder = new StringBuilder("SELECT x FROM (SELECT orderkey x, custkey y from orders limit 10) WHERE CASE true ");
        // Generate 100 cases.
        for (int i = 0; i < 100; i++) {
            stringBuilder.append(" when x in (");
            for (int j = 0; j < 20; j++) {
                stringBuilder.append("random(" + (i * 100 + j) + "), ");
            }

            stringBuilder.append("random(" + i + ")) then x = random()");
        }

        stringBuilder.append("else x = random() end");
        assertQueryFails(stringBuilder.toString(),
                "input > 0 \\(0 vs. 0\\) bound must be positive presto.default.random\\(0:INTEGER\\)", true);
    }

    @Override
    @Test
    public void testArraySplitIntoChunks()
    {
        String functionNotRegistered = "Scalar function name not registered: presto.default.array_split_into_chunks";

        String sql = "select array_split_into_chunks(array[1, 2, 3, 4, 5, 6], 2)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[1, 2, 3, 4, 5], 3)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[1, 2, 3], 5)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(null, 2)";
        assertQuery(sql, "values null");

        sql = "select array_split_into_chunks(array[1, 2, 3], 0)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[1, 2, 3], -1)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[1, null, 3, null, 5], 2)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array['a', 'b', 'c', 'd'], 2)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[1.1, 2.2, 3.3, 4.4, 5.5], 2)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[null, null, null], 0)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[null, null, null], 2)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[null, 1, 2], 5)";
        assertQueryFails(sql, functionNotRegistered, true);

        sql = "select array_split_into_chunks(array[], 0)";
        assertQueryFails(sql, functionNotRegistered, true);
    }

    @Test
    public void testAtTimeZone()
    {
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE INTERVAL '07:09' hour to minute", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT MIN(x) AT TIME ZONE 'America/Chicago' FROM (VALUES TIMESTAMP '1970-01-01 00:01:00+00:00') t(x)", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE '+07:09'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00 UTC' AT TIME ZONE 'America/Los_Angeles'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT x AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00', TIMESTAMP '1970-01-01 08:01:00', TIMESTAMP '1969-12-31 16:01:00') t(x)", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                timestampTzTypeUnsupportedError, true);

        // with chained AT TIME ZONE
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Tokyo' AT TIME ZONE 'America/Los_Angeles'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'Asia/Shanghai'", timestampTzTypeUnsupportedError, true);
        assertQueryFails("SELECT min(x) AT TIME ZONE 'America/Los_Angeles' AT TIME ZONE 'UTC' FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1969-12-31 16:01:00-08:00') t(x)",
                timestampTzTypeUnsupportedError, true);

        // with AT TIME ZONE in VALUES
        assertQueryFails("SELECT * FROM (VALUES TIMESTAMP '2012-10-31 01:00' AT TIME ZONE 'Asia/Oral')", timestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test
    public void testApproxMostFrequentWithVarchar()
    {
        MaterializedResult actual1 = computeActual("SELECT approx_most_frequent(3, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertEquals(actual1.getRowCount(), 1);
        // PRESTISSIMO_FIX
        assertEquals(actual1.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of("A", 3L, "B", 2L, "C", 2L));

        MaterializedResult actual2 = computeActual("SELECT approx_most_frequent(2, x, 15) FROM (values 'A', 'B', 'A', 'C', 'A', 'B', 'C', 'D', 'E') t(x)");
        assertEquals(actual2.getRowCount(), 1);
        // PRESTISSIMO_FIX
        assertEquals(actual2.getMaterializedRows().get(0).getFields().get(0), ImmutableMap.of("A", 3L, "C", 2L));
    }

    @Override
    @Test
    public void testMapUnionSumOverflow()
    {
        assertQueryFails(
                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,100] as array<tinyint>)) x " +
                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<tinyint>))x) group by y", "Value 200 exceeds 127", true);
        assertQueryFails(
                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30, 32760] as array<smallint>)) x " +
                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<smallint>))x) group by y", "Value 32860 exceeds 32767", true);
    }

    @Override
    @Test
    public void testRowSubscript()
    {
        // Subscript on Row with unnamed fields
        assertQuery("SELECT ROW (1, 'a', true)[2]", "SELECT 'a'");
        assertQuery("SELECT r[2] FROM (VALUES (ROW (ROW (1, 'a', true)))) AS v(r)", "SELECT 'a'");
        assertQuery("SELECT r[1], r[2] FROM (SELECT ROW (name, regionkey) FROM nation ORDER BY name LIMIT 1) t(r)", "VALUES ('ALGERIA', 0)");

        // Subscript on Row with named fields
        assertQuery("SELECT (CAST (ROW (1, 'a', 2 ) AS ROW (field1 bigint, field2 varchar(1), field3 bigint)))[2]", "SELECT 'a'");

        // Subscript on nested Row
        assertQuery("SELECT ROW (1, 'a', ROW (false, 2, 'b'))[3][3]", "SELECT 'b'");

        // Row subscript in filter condition
        assertQuery("SELECT orderstatus FROM orders WHERE ROW (orderkey, custkey)[1] = 100", "SELECT 'O'");

        // Row subscript in join condition
        assertQuery("SELECT n.name, r.name FROM nation n JOIN region r ON ROW (n.name, n.regionkey)[2] = ROW (r.name, r.regionkey)[2] ORDER BY n.name LIMIT 1", "VALUES ('ALGERIA', 'AFRICA')");

        //Row subscript in a lambda
        assertQueryFails("SELECT apply(ROW (1, 2), r -> r[2])", "Scalar function name not registered: presto.default.apply, called with arguments", true);
    }

    @Override
    @Test
    public void testDuplicateUnnestItem()
    {
        // unnest with cross join
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3]) AS r(r1, r2)", "VALUES (1, 2, 2), (1, 3, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) AS r(r1, r2, r3)", "VALUES (1, 2, 2, 2), (1, 3, 3, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) AS r(r1, r2, r3)", "VALUES (1, 2, 10, 2), (1, 3, 11, 3), (1, NULL, 12, NULL)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) AS r(r1, r2, r3)", "VALUES (1, 2, 2, 10), (1, 3, 3, 11), (1, NULL, NULL, 12)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) AS r(r1, r2, r3, r4)", "VALUES (1, 2, 'a', 2, 'a'), (1, 3, 'b', 3, 'b')");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) AS r(r1, r2, r3, r4, r5, r6)",
                "VALUES (1, 2, 'a', 1, 'a', 2, 'a'), (1, 3, 'b', 2, 'b', 3, 'b'), (1, NULL, NULL, 3, 'c', NULL, NULL)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])) AS r(r1, r2, r3, r4, r5, r6)",
                "VALUES (1, 2, 'a', 2, 'a', 1, 'a'), (1, 3, 'b', 3, 'b', 2, 'b'), (1, NULL, NULL, NULL, NULL, 3, 'c')");
        assertQuery("SELECT * from ( SELECT ARRAY[1] AS kv FROM (select 1)) CROSS JOIN UNNEST( kv, kv )", "VALUES (ARRAY[1], 1, 1)");

        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)", "VALUES (1, 2, 2, 1), (1, 3, 3, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 2, 2, 1), (1, 3, 3, 3, 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 10, 2, 1), (1, 3, 11, 3, 2), (1, NULL, 12, NULL, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (1, 2, 2, 10, 1), (1, 3, 3, 11, 2), (1, NULL, NULL, 12, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)", "VALUES (1, 2, 'a', 2, 'a', 1), (1, 3, 'b', 3, 'b', 2)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b'])) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, ord)",
                "VALUES (1, 2, 'a', 1, 'a', 2, 'a', 1), (1, 3, 'b', 2, 'b', 3, 'b', 2), (1, NULL, NULL, 3, 'c', NULL, NULL, 3)");
        assertQuery("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c'])) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, ord)",
                "VALUES (1, 2, 'a', 2, 'a', 1, 'a', 1), (1, 3, 'b', 3, 'b', 2, 'b', 2), (1, NULL, NULL, NULL, NULL, 3, 'c', 3)");
        assertQuery("SELECT * from ( SELECT ARRAY[1] AS kv FROM (select 1)) CROSS JOIN UNNEST( kv, kv ) WITH ORDINALITY AS t(r1, r2, ord)", "VALUES (ARRAY[1], 1, 1, 1)");
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3, r4, r5, r6, r7)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, r7, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);

        Session useLegacyUnnest = Session.builder(getSession())
                .setSystemProperty(LEGACY_UNNEST, "true")
                .build();
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)) from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2)",
                "VALUES (1, row(2, 3), row(2, 3)), (1, row(3, 5), row(3, 5))");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)) from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3)",
                "VALUES (1, row(2, 3), row(2, 3), row(10, 13, 15)), (1, row(3, 5), row(3, 5), row(23, 25, 20))");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), ord from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, ord)",
                "VALUES (1, row(2, 3), row(2, 3), 1), (1, row(3, 5), row(3, 5), 2)");
        assertQuery(useLegacyUnnest, "SELECT k, cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)), ord from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, ord)",
                "VALUES (1, row(2, 3), row(2, 3), row(10, 13, 15), 1), (1, row(3, 5), row(3, 5), row(23, 25, 20), 2)");

        assertQuery("select orderkey, custkey, totalprice, t1, t2 from orders cross join unnest(array[custkey], array[custkey]) as t(t1, t2)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2 from orders");
        assertQuery("select orderkey, custkey, totalprice, t1, t2, t3 from orders cross join unnest(array[custkey], array[custkey], array[custkey]) as t(t1, t2, t3)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2, custkey as t3 from orders");
        assertQuery("select orderkey, custkey, totalprice, t1, t2, t3 from orders cross join unnest(array[custkey], array[custkey], array[shippriority]) as t(t1, t2, t3)",
                "select orderkey, custkey, totalprice, custkey as t1, custkey as t2, shippriority as t3 from orders");

        // SIMPLE UNNEST without cross join
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3])", "VALUES (2, 2), (3, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3])", "VALUES (2, 2, 2), (3, 3, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3])", "VALUES (2, 10, 2), (3, 11, 3), (NULL, 12, NULL)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12])", "VALUES (2, 2, 10), (3, 3, 11), (NULL, NULL, 12)");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']))", "VALUES (2, 'a', 2, 'a'), (3, 'b', 3, 'b')");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']), MAP(ARRAY[2, 3], ARRAY['a', 'b']))",
                "VALUES (2, 'a', 1, 'a', 2, 'a'), (3, 'b', 2, 'b', 3, 'b'), (NULL, NULL, 3, 'c', NULL, NULL)");
        assertQuery("SELECT * from unnest(MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[2, 3], ARRAY['a', 'b']), MAP(ARRAY[1, 2, 3], ARRAY['a', 'b', 'c']))",
                "VALUES (2, 'a', 2, 'a', 1, 'a'), (3, 'b', 3, 'b', 2, 'b'), (NULL, NULL, NULL, NULL, 3, 'c')");

        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)", "VALUES (2, 2, 1), (3, 3, 2)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 2, 2, 1), (3, 3, 3, 2)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[10,11,12], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 10, 2, 1), (3, 11, 3, 2), (NULL, 12, NULL, 3)");
        assertQuery("SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3], ARRAY[10,11,12]) WITH ORDINALITY AS r(r1, r2, r3, ord)", "VALUES (2, 2, 10, 1), (3, 3, 11, 2), (NULL, NULL, 12, 3)");

        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);

        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)) from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2)",
                "VALUES (row(2, 3), row(2, 3)), (row(3, 5), row(3, 5))");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)) from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3)",
                "VALUES (row(2, 3), row(2, 3), row(10, 13, 15)), (row(3, 5), row(3, 5), row(23, 25, 20))");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), ord from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, ord)",
                "VALUES (row(2, 3), row(2, 3), 1), (row(3, 5), row(3, 5), 2)");
        assertQuery(useLegacyUnnest, "SELECT cast(r1 as row(x int, y int)), cast(r2 as row(x int, y int)), cast(r3 as row(x int, y int, z int)), ord from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, ord)",
                "VALUES (row(2, 3), row(2, 3), row(10, 13, 15), 1), (row(3, 5), row(3, 5), row(23, 25, 20), 2)");

        // mixed
        assertQuery("select * from (SELECT * from unnest(ARRAY[2, 3], ARRAY[2, 3]) WITH ORDINALITY AS r(r1, r2, ord)) cross join unnest(ARRAY[2, 3], ARRAY[2, 3])",
                "VALUES (2, 2, 1, 2, 2), (2, 2, 1, 3, 3), (3, 3, 2, 2, 2), (3, 3, 2, 3, 3)");
    }

    @Override
    @Test(dataProvider = "optimize_hash_generation")
    public void testDoubleDistinctPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQueryFails(session, "SELECT DISTINCT x FROM (VALUES (DOUBLE '0.0'), (DOUBLE '-0.0')) t(x)",
                hashGenerationUnsupportedError, true);
    }

    @Override
    @Test
    public void testIn()
    {
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1.5, 2.3)", "SELECT orderkey FROM orders LIMIT 0"); // H2 incorrectly matches rows
        assertQuery("SELECT orderkey FROM orders WHERE orderkey IN (1, 2E0, 3)");
        assertQuery("SELECT orderkey FROM orders WHERE totalprice IN (1, 2, 3)");
        assertQuery("SELECT x FROM (values 3, 100) t(x) WHERE x IN (2147483649)", "SELECT * WHERE false");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (2147483648, 2147483650)", "values 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 3, 100, 2147483648, 2147483649, 2147483650) t(x) WHERE x IN (3, 4, 2147483648, 2147483650)", "values 3, 2147483648, 2147483650");
        assertQuery("SELECT x FROM (values 1, 2, 3) t(x) WHERE x IN (1 + CAST(rand() < 0 AS bigint), 2 + CAST(rand() < 0 AS bigint))", "values 1, 2");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (1 + CAST(rand() < 0 AS bigint), 2 + CAST(rand() < 0 AS bigint), 4)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 4) t(x) WHERE x IN (4, 2, 1)", "values 1, 2, 4");
        assertQuery("SELECT x FROM (values 1, 2, 3, 2147483648) t(x) WHERE x IN (1 + CAST(rand() < 0 AS bigint), 2 + CAST(rand() < 0 AS bigint), 2147483648)", "values 1, 2, 2147483648");
        assertQuery("SELECT x IN (0) FROM (values 4294967296) t(x)", "values false");
        assertQuery("SELECT x IN (0, 4294967297 + CAST(rand() < 0 AS bigint)) FROM (values 4294967296, 4294967297) t(x)", "values false, true");
        assertQuery("SELECT NULL in (1, 2, 3)", "values null");
        assertQuery("SELECT 1 in (1, NULL, 3)", "values true");
        assertQuery("SELECT 2 in (1, NULL, 3)", "values null");
        assertQuery("SELECT x FROM (values DATE '1970-01-01', DATE '1970-01-03') t(x) WHERE x IN (DATE '1970-01-01')", "values DATE '1970-01-01'");
        assertQueryFails("SELECT x FROM (values TIMESTAMP '1970-01-01 00:01:00+00:00', TIMESTAMP '1970-01-01 08:01:00+08:00', TIMESTAMP '1970-01-01 00:01:00+08:00') t(x) WHERE x IN (TIMESTAMP '1970-01-01 00:01:00+00:00')",
                        timestampTzTypeUnsupportedError, true);
        assertQuery("SELECT COUNT(*) FROM (values 1) t(x) WHERE x IN (null, 0)", "SELECT 0");
        assertQuery("SELECT d IN (DECIMAL '2.0', DECIMAL '30.0') FROM (VALUES (2.0E0)) t(d)", "SELECT true"); // coercion with type only coercion inside IN list
    }

    @Override
    @Test
    public void testPreserveAssignmentsInJoin()
    {
        // The following two timestamps represent the same point in time but with different time zones
        String timestampLosAngeles = "2001-08-22 03:04:05.321 America/Los_Angeles";
        String timestampNewYork = "2001-08-22 06:04:05.321 America/New_York";
        String sql = "WITH source AS (" +
                "SELECT * FROM (" +
                "    VALUES" +
                "        (TIMESTAMP '" + timestampLosAngeles + "')," +
                "        (TIMESTAMP '" + timestampNewYork + "')" +
                ") AS tbl (tstz)" +
                ")" +
                "SELECT * FROM source a JOIN source b ON a.tstz = b.tstz";
        assertQueryFails(sql, timestampTzTypeUnsupportedError, true);
    }

    @Override
    @Test(dataProvider = "optimize_hash_generation")
    public void testRealDistinctPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQueryFails(session, "SELECT DISTINCT x FROM (VALUES (REAL '0.0'), (REAL '-0.0')) t(x)",
                hashGenerationUnsupportedError, true);
    }

    @Override
    @Test(dataProvider = "optimize_hash_generation")
    public void testRealJoinPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQueryFails(session, "WITH t AS ( SELECT * FROM (VALUES(REAL '0.0'), (REAL '-0.0'))_t(x)) SELECT * FROM t t1 JOIN t t2 on t1.x = t2.x",
                hashGenerationUnsupportedError, true);
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey) c FROM orders UNION ALL SELECT empty_approx_set())",
                "Scalar function name not registered: presto.default.create_hll, called with arguments: \\(BIGINT\\)", true);
    }

    @Override
    @Test
    public void testSetUnion()
    {
        // sanity
        assertQuery(
                "select set_union(x) from (values array[1, 2], array[3, 4], array[5, 6]) as t(x)",
                "select array[1, 2, 3, 4, 5, 6]");
        assertQuery(
                "select set_union(x) from (values array[1, 2, 3], array[2, 3, 4], array[7, 8]) as t(x)",
                "select array[1, 2, 3, 4, 7, 8]");
        assertQuery(
                "select group_id, set_union(numbers) from (values (1, array[1, 2]), (1, array[2, 3]), (2, array[4, 5]), (2, array[5, 6])) as t(group_id, numbers) group by group_id",
                "select group_id, numbers from (values (1, array[1, 2, 3]), (2, array[4, 5, 6])) as t(group_id, numbers)");
        assertQuery(
                "select group_id, set_union(numbers) from (values (1, array[1, 2]), (2, array[2, 3]), (3, array[4, 5]), (4, array[5, 6])) as t(group_id, numbers) group by group_id",
                "select group_id, numbers from (values (1, array[1, 2]), (2, array[2, 3]), (3, array[4, 5]), (4, array[5, 6])) as t(group_id, numbers)");
        // all nulls should return empty array to match behavior of array_distinct(flatten(array_agg(x)))
        assertQueryFails(
                "select set_union(x) from (values null, null, null) as t(x)",
                "Unexpected type UNKNOWN", true);
        // nulls inside arrays should be captured while pure nulls should be ignored
        assertQueryFails(
                "select set_union(x) from (values null, array[null], null) as t(x)",
                "Unexpected type UNKNOWN", true);
        // null inside arrays should be captured
        assertQuery(
                "select set_union(x) from (values array[1, 2, 3], array[null], null) as t(x)",
                "select array[1, 2, 3, null]");
        // return null for empty rows
        assertQueryFails(
                "select set_union(x) from (values null, array[null], null) as t(x) where x != null",
                "Unexpected type UNKNOWN", true);
    }

    @Override
    @Test
    public void testSamplingJoinChain()
    {
        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .build();
        String query = "select count(1) FROM lineitem l left JOIN orders o ON l.orderkey = o.orderkey JOIN customer c ON o.custkey = c.custkey";

        assertQuery(query, "select 60175");
        assertQueryFails(sessionWithKeyBasedSampling, query, "Scalar function name not registered: presto.default.key_sampling_percent, called with arguments", true);
    }

    @Override
    @Test
    public void testApproxSetBigint()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey)) FROM orders");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1005L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetBigintGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(custkey)) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1003L)
                .row("F", 1001L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetDouble()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS DOUBLE))) FROM orders");

        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1002L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetDoubleGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS DOUBLE))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1002L)
                .row("F", 998L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetGroupByWithNulls()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(custkey % 2 <> 0, custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 499L)
                .row("F", 496L)
                .row("P", 153L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetVarchar()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(CAST(custkey AS VARCHAR))) FROM orders");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1015L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetVarcharGroupBy()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(CAST(custkey AS VARCHAR))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", 1012L)
                .row("F", 1011L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetWithNulls()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(IF(orderstatus = 'O', custkey))) FROM orders");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row(1003L)
                .build();

        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        MaterializedResult actual = computeActual("" +
                "SELECT orderstatus, cardinality(approx_set(IF(orderstatus != 'O', custkey))) " +
                "FROM orders " +
                "GROUP BY orderstatus");

        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 1001L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testReduceAggWithNulls()
    {
        assertQueryFails("select reduce_agg(x, null, (x,y)->try(x+y), (x,y)->try(x+y)) from (select 1 union all select 10) T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
        assertQueryFails("select reduce_agg(x, cast(null as bigint), (x,y)->coalesce(x, 0)+coalesce(y, 0), (x,y)->coalesce(x, 0)+coalesce(y, 0)) from (values cast(10 as bigint),10)T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");

        // here some reduce_aggs coalesce overflow/zero-divide errors to null in the input/combine functions
        assertQuery("select reduce_agg(x, 0, (x,y)->try(1/x+1/y), (x,y)->try(1/x+1/y)) from ((select 0) union all select 10.) T(x)", "select 0.0");
        assertQueryFails("select reduce_agg(x, 0, (x, y)->try(x+y), (x, y)->try(x+y)) from (values 2817, 9223372036854775807) AS T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQuery("select reduce_agg(x, array[], (x, y)->array[element_at(x, 2)],  (x, y)->array[element_at(x, 2)]) from (select array[array[1]]) T(x)", "select array[null]");
    }

    @Override
    @Test(expectedExceptions = {RuntimeException.class, PrestoException.class}, expectedExceptionsMessageRegExp = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*")
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.2))");
    }

    //@Test @Override
    @Override
    @Test(expectedExceptions = {RuntimeException.class, PrestoException.class}, expectedExceptionsMessageRegExp = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*")
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.1))");
        // PRESTISSIMO_FIX
        MaterializedResult expected = resultBuilder(getSession(), BIGINT)
                .row(1046L)
                .build();
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testRows()
    {
        // Using JSON_FORMAT(CAST(_ AS JSON)) because H2 does not support ROW type
        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))", "SELECT '{\"\":3,\"\":\"ab\"}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(a + b) AS JSON)) FROM (VALUES (1, 2)) AS t(a, b)", "SELECT '[3]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1, ROW(9, a, ARRAY[], NULL), ROW(1, 2)) AS JSON)) FROM (VALUES ('a')) t(a)",
                "SELECT '[1,[9,\"a\",[],null],[1,2]]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(ROW(ROW(ROW(ROW(a, b), c), d), e), f) AS JSON)) FROM (VALUES (ROW(0, 1), 2, '3', NULL, ARRAY[5], ARRAY[])) t(a, b, c, d, e, f)",
                "SELECT '[[[[[[0,1],2],\"3\"],null],[5]],[]]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(a, b)) AS JSON)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)",
                "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery(session, "SELECT CONTAINS(ARRAY_AGG(ROW(a, b)), ROW(1, 2)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT TRUE");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(c, d)) AS JSON)) FROM (VALUES (ARRAY[1, 3, 5], ARRAY[2, 4, 6])) AS t(a, b) CROSS JOIN UNNEST(a, b) AS u(c, d)",
                "SELECT '[[1,2],[3,4],[5,6]]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, NULL, '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, CAST(NULL AS INTEGER), '3')) t(x,y,z)", "SELECT '[1,null,\"3\"]'");
    }

    @Override
    @Test(enabled = false)
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
        String subqueryReturnedTooManyRows = ".*Scalar sub-query has returned multiple rows.*";

        assertQuery("SELECT (SELECT 1 WHERE a = 2) FROM (VALUES 1) t(a)", "SELECT null");
        assertQuery("SELECT (SELECT 2 WHERE a = 1) FROM (VALUES 1) t(a)", "SELECT 2");
        assertQueryFails(
                "SELECT (SELECT 2 FROM (VALUES 3, 4) WHERE a = 1) FROM (VALUES 1) t(a)",
                subqueryReturnedTooManyRows);

        // multiple subquery output projections
        // TODO: Check why expected query runner throws an error but native query runner doesn't.
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT 'bleh' FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        assertQueryFails(
                "SELECT name FROM nation n WHERE 1 = (SELECT 1 FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);

        // correlation used in subquery output
        assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT n.name FROM region WHERE regionkey > n.regionkey)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertQuery(
                "SELECT (SELECT 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                "VALUES 2, null, null, null, null");
        // outputs plain correlated orderkey symbol which causes ambiguity with outer query orderkey symbol
        assertQueryFails(
                "SELECT (SELECT o.orderkey WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        assertQueryFails(
                "SELECT (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
        // correlation used outside the subquery
        assertQueryFails(
                "SELECT o.orderkey, (SELECT o.orderkey * 2 WHERE o.orderkey = 1) FROM orders o ORDER BY orderkey LIMIT 5",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // aggregation with having
//        TODO: uncomment below test once #8456 is fixed
//        assertQuery("SELECT (SELECT avg(totalprice) FROM orders GROUP BY custkey, orderdate HAVING avg(totalprice) < a) FROM (VALUES 900) t(a)");

        // correlation in predicate
        assertQueryFails("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey = n.regionkey)", failNotRegisteredError, true);

        // same correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.regionkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // different correlation in predicate and projection
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT n.nationkey * 2 FROM region r WHERE n.regionkey = r.regionkey) > 6",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // correlation used in subrelation
        assertQueryFails(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT regionkey * 2 FROM (SELECT regionkey FROM region r WHERE n.regionkey = r.regionkey)) > 6 " +
                        "ORDER BY 1 LIMIT 3", failNotRegisteredError, true);

        // with duplicated rows
        assertQueryFails(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 1, 1, 2, 3) t(a)", failNotRegisteredError, true);

        // returning null when nothing matched
        assertQueryFails(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 31) t(a)", failNotRegisteredError, true);

        assertQueryFails(
                "SELECT (SELECT r.name FROM nation n, region r WHERE r.regionkey = n.regionkey AND n.nationkey = a) FROM (VALUES 1) t(a)", failNotRegisteredError, true);
    }

    @Override
    @Test(enabled = false)
    public void testScalarSubquery()
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
        // TODO: Investigate error 'Caused by: java.lang.IllegalArgumentException: expected types count (22) does not
        //  match actual column count (16)'
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is null");
        assertQuery("SELECT * FROM lineitem WHERE \n" +
                "(SELECT orderkey FROM orders WHERE 0=1) " +
                "is not null");

        // subquery results and in in-predicate
        assertQuery("SELECT (SELECT 1) IN (1, 2, 3)");
        assertQuery("SELECT (SELECT 1) IN (   2, 3)");

        // multiple subqueries
        assertQuery("SELECT (SELECT 1) = (SELECT 3)");
        assertQuery("SELECT (SELECT 1) < (SELECT 3)");
        assertQuery("SELECT COUNT(*) FROM lineitem WHERE " +
                "(SELECT min(orderkey) FROM orders)" +
                "<" +
                "(SELECT max(orderkey) FROM orders)");
        assertQuery("SELECT (SELECT 1), (SELECT 2), (SELECT 3)");

        // distinct
        assertQuery("SELECT DISTINCT orderkey FROM lineitem " +
                "WHERE orderkey BETWEEN" +
                "   (SELECT avg(orderkey) FROM orders) - 10 " +
                "   AND" +
                "   (SELECT avg(orderkey) FROM orders) + 10");

        // subqueries with joins
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 " +
                "INNER JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                "LEFT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT o1.orderkey, COUNT(*) " +
                "FROM orders o1 RIGHT JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                "ON o1.orderkey " +
                "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                "GROUP BY o1.orderkey");
        assertQuery("SELECT DISTINCT COUNT(*) " +
                        "FROM (SELECT * FROM orders ORDER BY orderkey LIMIT 5) o1 " +
                        "FULL JOIN (SELECT * FROM orders ORDER BY orderkey LIMIT 10) o2 " +
                        "ON o1.orderkey " +
                        "BETWEEN (SELECT avg(orderkey) FROM orders) - 10 AND (SELECT avg(orderkey) FROM orders) + 10 " +
                        "GROUP BY o1.orderkey",
                "VALUES 1, 10");

        // subqueries with ORDER BY
        assertQuery("SELECT orderkey, totalprice FROM orders ORDER BY (SELECT 2)");

        // subquery returns multiple rows
        String multipleRowsErrorMsg = "Scalar sub-query has returned multiple rows";
        assertQueryFails("SELECT * FROM lineitem WHERE orderkey = (\n" +
                        "SELECT orderkey FROM orders ORDER BY totalprice)",
                multipleRowsErrorMsg);
        assertQueryFails("SELECT orderkey, totalprice FROM orders ORDER BY (VALUES 1, 2)",
                multipleRowsErrorMsg);

        // exposes a bug in optimize hash generation because EnforceSingleNode does not
        // support more than one column from the underlying query
        assertQuery("SELECT custkey, (SELECT DISTINCT custkey FROM orders ORDER BY custkey LIMIT 1) FROM orders");

        // cast scalar sub-query
        assertQuery("SELECT 1.0/(SELECT 1), CAST(1.0 AS REAL)/(SELECT 1), 1/(SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1) AND 1 = (SELECT 1), 2.0 = (SELECT 1) WHERE 1.0 = (SELECT 1) AND 1 = (SELECT 1)");
        assertQuery("SELECT 1.0 = (SELECT 1), 2.0 = (SELECT 1), CAST(2.0 AS REAL) = (SELECT 1) WHERE 1.0 = (SELECT 1)");

        // coerce correlated symbols
        assertQuery("SELECT * FROM (VALUES 1) t(a) WHERE 1=(SELECT count(*) WHERE 1.0 = a)", "SELECT 1");
        assertQuery("SELECT * FROM (VALUES 1.0) t(a) WHERE 1=(SELECT count(*) WHERE 1 = a)", "SELECT 1.0");
    }

    @Override
    @Test(dataProvider = "optimize_hash_generation")
    public void testDoubleJoinPositiveAndNegativeZero(String optimizeHashGeneration)
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, optimizeHashGeneration)
                .build();
        assertQueryFails(session, "WITH t AS ( SELECT * FROM (VALUES(DOUBLE '0.0'), (DOUBLE '-0.0'))_t(x)) SELECT * FROM t t1 JOIN t t2 on t1.x = t2.x",
                hashGenerationUnsupportedError, true);
    }

    @Override
    public void testCorrelatedScalarSubqueries()
    {
        assertQuery("SELECT (SELECT n.nationkey + n.NATIONKEY) FROM nation n");
        assertQuery("SELECT (SELECT 2 * n.nationkey) FROM nation n");
        assertQuery("SELECT nationkey FROM nation n WHERE 2 = (SELECT 2 * n.nationkey)");
        assertQuery("SELECT nationkey FROM nation n ORDER BY (SELECT 2 * n.nationkey)");

        // group by
        assertQuery("SELECT max(n.regionkey), 2 * n.nationkey, (SELECT n.nationkey) FROM nation n GROUP BY n.nationkey");
        assertQuery(
                "SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey HAVING max(l.quantity) < (SELECT l.orderkey)");
        assertQuery("SELECT max(l.quantity), 2 * l.orderkey FROM lineitem l GROUP BY l.orderkey, (SELECT l.orderkey)");

        // join
        assertQuery("SELECT * FROM nation n1 JOIN nation n2 ON n1.nationkey = (SELECT n2.nationkey)");
        assertQueryFails(
                "SELECT (SELECT l3.* FROM lineitem l2 CROSS JOIN (SELECT l1.orderkey) l3 LIMIT 1) FROM lineitem l1",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        // subrelation
        assertQuery(
                "SELECT 1 FROM nation n WHERE 2 * nationkey - 1  = (SELECT * FROM (SELECT n.nationkey))",
                "SELECT 1"); // h2 fails to parse this query

        // two level of nesting
        assertQuery("SELECT * FROM nation n WHERE 2 = (SELECT (SELECT 2 * n.nationkey))");

        // explicit LIMIT in subquery
        assertQueryFails(
                "SELECT (SELECT count(*) FROM (VALUES (7,1)) t(orderkey, value) WHERE orderkey = corr_key LIMIT 1) FROM (values 7) t(corr_key)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);

        assertQuery(
                "SELECT (SELECT count(*) FROM (VALUES (7,1)) t(orderkey, value) WHERE orderkey = corr_key GROUP BY value LIMIT 2) FROM (values 7) t(corr_key)");

        // Limit(1) and non-constant output symbol of the subquery (count)
        assertQueryFails("SELECT (SELECT count(*) FROM (VALUES (7,1), (7,2)) t(orderkey, value) WHERE orderkey = corr_key GROUP BY value LIMIT 1) FROM (values 7) t(corr_key)",
                UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG);
    }

    @Override
    public void testLikePrefixAndSuffixWithChars()
    {
        assertQueryFails("select x like 'abc%' from (values CAST ('abc' AS CHAR(3)), CAST ('def' AS CHAR(3)), CAST ('bcd' AS CHAR(3))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails("select x like '%abc%' from (values CAST ('xabcy' AS CHAR(5)), CAST ('abxabcdef' AS CHAR(9)), CAST ('bcd' AS CHAR(3)),  CAST ('xabcyabcz' AS CHAR(9))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails(
                "select x like '%abc' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4)), CAST ('xabc' AS CHAR(4)), CAST (' xabc' AS CHAR(5))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails("select x like '%ab_c' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails("select x like '%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails("select x like '%_%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails("select x like '%a%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", chartimestampTzTypeUnsupportedError, true);
        assertQueryFails("select x like '%acd%xy%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", chartimestampTzTypeUnsupportedError, true);
    }

    @Override
    public void testMapBlockBug()
    {
        assertQueryFails(" VALUES(MAP_AGG(12345,123))", "Scalar function name not registered: presto.default.map_agg", true);
    }

    @Override
    public void testMergeKHyperLogLog()
    {
        assertQueryFails("select k1, cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1", kp4HlltimestampTzTypeUnsupportedError, true);

        assertQueryFails("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)", kp4HlltimestampTzTypeUnsupportedError, true);
    }

    @Override
    public void testReduceAgg()
    {
        assertQuery(
                "SELECT x, reduce_agg(y, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM (VALUES (1, 5), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 5 * 6 * 7), (2, 8 * 9), (3, 10)");
        assertQuery(
                "SELECT x, reduce_agg(y, 0, (a, b) -> a + b, (a, b) -> a + b) " +
                        "FROM (VALUES (1, 5), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 5 + 6 + 7), (2, 8 + 9), (3, 10)");

        assertQuery(
                "SELECT x, reduce_agg(y, 1, (a, b) -> a * b, (a, b) -> a * b) " +
                        "FROM (VALUES (1, CAST(5 AS DOUBLE)), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, CAST(5 AS DOUBLE) * 6 * 7), (2, 8 * 9), (3, 10)");
        assertQuery(
                "SELECT x, reduce_agg(y, 0, (a, b) -> a + b, (a, b) -> a + b) " +
                        "FROM (VALUES (1, CAST(5 AS DOUBLE)), (1, 6), (1, 7), (2, 8), (2, 9), (3, 10)) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, CAST(5 AS DOUBLE) + 6 + 7), (2, 8 + 9), (3, 10)");

        assertQuery(
                "SELECT " +
                        "x, " +
                        "array_join(" +
                        "   array_sort(" +
                        "       split(reduce_agg(y, '', (a, b) -> a || b, (a, b) -> a || b), '')" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, 'a'), (1, 'b'), (1, 'c'), (2, 'd'), (2, 'e'), (3, 'f')) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abc'), (2, 'de'), (3, 'f')");

        assertQuery(
                "SELECT " +
                        "x, " +
                        "array_join(" +
                        "   array_sort(" +
                        "       array['x'] || reduce_agg(y, ARRAY[''], (a, b) -> a || b, (a, b) -> a || b)" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, ARRAY['a']), (1, ARRAY['b']), (1, ARRAY['c']), (2, ARRAY['d']), (2, ARRAY['e']), (3, ARRAY['f'])) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abcx'), (2, 'dex'), (3, 'fx')");

        assertQuery("SELECT REDUCE_AGG((x,y), (0,0), (x, y)->(x[1],y[1]), (x,y)->(x[1],y[1]))[1] from (select 1 x, 2 y)", "select 0");
    }

    @Override
    @Test
    public void testRemoveMapCast()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                "values 0.5, null");
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                "Cannot cast BIGINT.*to INTEGER. Overflow during arithmetic conversion", true);
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), cast('2' as varchar)), (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), '4')) t(feature,  key)",
                "values 0.5, 0.1");
    }

    @Override
    @Test(enabled = false)
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "false")
                .build();
        // Trigger optimization
        // TODO: Results do not match, investigate further.
        assertQuery(session, "select * from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select *, cast(o.custkey as varchar) from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select *, cast(o.custkey as varchar), cast(c.custkey as varchar) from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select r.custkey, r.orderkey, r.name, n.nationkey from (select o.custkey, o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)) r, nation n");
        // Do not trigger optimization
        assertQuery(session, "select * from customer c join orders o on cast(acctbal as varchar) = cast(totalprice as varchar)");
    }

    @Override
    @Test(enabled = false)
    public void testSameAggregationWithAndWithoutFilter()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "true")
                .build();
        Session disableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "false")
                .build();

        assertQuery(enableOptimization, "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey", "values (3,-6799976246779207259,5),(2,5,5),(0,-6799976246779207262,5),(4,-6799976246779207261,5),(1,-6799976246779207260,5)");
        assertQuery(enableOptimization, "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation", "values (15,25)");
        assertQuery(enableOptimization, "select count(1), count(1) filter (where k > 5) from (values 1, null, 3, 5, null, 8, 10) t(k)", "values (7, 2)");

        String sql = "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey";
        MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
        MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);

        // TODO: Results do not match, investigate further.
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount > 0.1) from lineitem group by grouping sets((), (partkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

        // multiple aggregations in query
        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount < 0.05), sum(linenumber), sum(linenumber) filter (where discount < 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregations in multiple levels
        sql = "select partkey, avg(sum), avg(sum) filter (where tax < 0.05), avg(filtersum) from (select partkey, suppkey, sum(quantity) sum, sum(quantity) filter (where discount > 0.05) filtersum, max(tax) tax from lineitem where partkey=1598 group by partkey, suppkey) t group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // global aggregation
        sql = "select sum(quantity), sum(quantity) filter (where discount < 0.05) from lineitem";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // order by
        sql = "select partkey, array_agg(suppkey order by suppkey), array_agg(suppkey order by suppkey) filter (where discount > 0.05) from lineitem group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // grouping sets
        sql = "SELECT partkey, suppkey, sum(quantity), sum(quantity) filter (where discount > 0.05) from lineitem group by grouping sets((), (partkey), (partkey, suppkey))";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over union
        sql = "SELECT partkey, sum(quantity), sum(quantity) filter (where orderkey > 0) from (select quantity, orderkey, partkey from lineitem union all select totalprice as quantity, orderkey, custkey as partkey from orders) group by partkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
        // aggregation over join
        sql = "select custkey, sum(quantity), sum(quantity) filter (where tax < 0.05) from lineitem l join orders o on l.orderkey=o.orderkey group by custkey";
        resultWithOptimization = computeActual(enableOptimization, sql);
        resultWithoutOptimization = computeActual(disableOptimization, sql);
        assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);
    }

    @Override
    @Test
    public void testSetAggIndeterminateArrays()
    {
        // union all is to force usage of the serialized state
        assertQueryFails("SELECT unnested from (SELECT set_agg(x) as agg_result from (" +
                        "SELECT ARRAY[ARRAY[null, 2]] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, ARRAY[1, null]] " +
                        "UNION ALL " +
                        "SELECT ARRAY[ARRAY[null, 2]])) " +
                        "CROSS JOIN unnest(agg_result) as r(unnested)",
                arrayComparisonUnsupportedError, true);
    }

    @Override
    @Test
    public void testSetAggIndeterminateRows()
    {
        // union all is to force usage of the serialized state
        assertQueryFails("SELECT unnested from (SELECT set_agg(x) as agg_result from (" +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, CAST(row(1, null) AS ROW(INTEGER, INTEGER))] " +
                        "UNION ALL " +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))])) " +
                        "CROSS JOIN unnest(agg_result) as r(unnested)",
                arrayComparisonUnsupportedError, true);
    }

    @Override
    @Test
    public void testSetUnionIndeterminateRows()
    {
        // union all is to force usage of the serialized state
        assertQueryFails("SELECT c1, c2 from (SELECT set_union(x) as agg_result from (" +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))] x " +
                        "UNION ALL " +
                        "SELECT ARRAY[null, CAST(row(1, null) AS ROW(INTEGER, INTEGER))] " +
                        "UNION ALL " +
                        "SELECT ARRAY[CAST(row(null, 2) AS ROW(INTEGER, INTEGER))])) " +
                        "CROSS JOIN unnest(agg_result) as r(c1, c2)",
                ".*Field not found.*", true);
    }

    @Override
    @Test
    public void testTry()
    {
        // divide by zero
        assertQuery(
                "SELECT linenumber, sum(TRY(100/(CAST (tax*10 AS BIGINT)))) FROM lineitem GROUP BY linenumber",
                "SELECT linenumber, sum(100/(CAST (tax*10 AS BIGINT))) FROM lineitem WHERE CAST(tax*10 AS BIGINT) <> 0 GROUP BY linenumber");

        // invalid cast
        assertQuery(
                "SELECT TRY(CAST(IF(round(totalprice) % 2 = 0, CAST(totalprice AS VARCHAR), '^&$' || CAST(totalprice AS VARCHAR)) AS DOUBLE)) FROM orders",
                "SELECT CASE WHEN round(totalprice) % 2 = 0 THEN totalprice ELSE null END FROM orders");

        // invalid function argument
        assertQuery(
                "SELECT COUNT(TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // as part of a complex expression
        assertQuery(
                "SELECT COUNT(CAST(orderkey AS VARCHAR) || TRY(to_base(100, CAST(round(totalprice/100) AS BIGINT)))) FROM orders",
                "SELECT SUM(CASE WHEN CAST(round(totalprice/100) AS BIGINT) BETWEEN 2 AND 36 THEN 1 ELSE 0 END) FROM orders");

        // missing function argument
        assertQueryFails("SELECT TRY()", "line 1:8: The 'try' function must have exactly one argument");

        // check that TRY is not pushed down
        assertQueryFails("SELECT TRY(x) IS NULL FROM (SELECT 1/y AS x FROM (VALUES 1, 2, 3, 0, 4) t(y))", ".*division by zero Top-level Expression.*presto.default.divide.*");
        assertQuery("SELECT x IS NULL FROM (SELECT TRY(1/y) AS x FROM (VALUES 3, 0, 4) t(y))", "VALUES false, true, false");

        // test try with lambda function
        assertQueryFails("SELECT TRY(apply(5, x -> x + 1) / 0)", "Scalar function name not registered: presto.default.apply", true);
        assertQueryFails("SELECT TRY(apply(5 + RANDOM(1), x -> x + 1) / 0)", "Scalar function name not registered: presto.default.apply", true);
        assertQueryFails("SELECT apply(5 + RANDOM(1), x -> x + TRY(1 / 0))", applyNotRegisteredError, true);

        // test try with invalid JSON
        assertQuery("SELECT JSON_FORMAT(TRY(JSON 'INVALID'))", "SELECT NULL");
        assertQuery("SELECT JSON_FORMAT(TRY (JSON_PARSE('INVALID')))", "SELECT NULL");

        // tests that might be constant folded
        assertQuery("SELECT TRY(CAST(NULL AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST('123' AS BIGINT))", "SELECT 123L");
        assertQuery("SELECT TRY(CAST('foo' AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST('foo' AS BIGINT)) + TRY(CAST('123' AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT TRY(CAST(CAST(123 AS VARCHAR) AS BIGINT))", "SELECT 123L");
        assertQuery("SELECT COALESCE(CAST(CONCAT('123', CAST(123 AS VARCHAR)) AS BIGINT), 0)", "SELECT 123123L");
        assertQuery("SELECT TRY(CAST(CONCAT('hello', CAST(123 AS VARCHAR)) AS BIGINT))", "SELECT NULL");
        assertQuery("SELECT COALESCE(TRY(CAST(CONCAT('a', CAST(123 AS VARCHAR)) AS INTEGER)), 0)", "SELECT 0");
        assertQuery("SELECT COALESCE(TRY(CAST(CONCAT('a', CAST(123 AS VARCHAR)) AS BIGINT)), 0)", "SELECT 0L");
        assertQuery("SELECT 123 + TRY(ABS(-9223372036854775807 - 1))", "SELECT NULL");
        assertQuery("SELECT JSON_FORMAT(TRY(JSON '[]')) || '123'", "SELECT '[]123'");
        assertQuery("SELECT JSON_FORMAT(TRY(JSON 'INVALID')) || '123'", "SELECT NULL");
        assertQuery("SELECT TRY(2/1)", "SELECT 2");
        assertQuery("SELECT TRY(2/0)", "SELECT null");
        assertQuery("SELECT COALESCE(TRY(2/0), 0)", "SELECT 0");
        assertQuery("SELECT TRY(ABS(-2))", "SELECT 2");

        // test try with null
        assertQuery("SELECT TRY(1 / x) FROM (SELECT NULL as x)", "SELECT NULL");
    }

    @Override
    @Test
    public void testLambdaInAggregation()
    {
        assertQuery("SELECT id, reduce_agg(value, 0, (a, b) -> a + b+0, (a, b) -> a + b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id", "values (1, 9), (2, 90)");
        assertQuery("SELECT id, 's' || reduce_agg(value, '', (a, b) -> concat(a, b, 's'), (a, b) -> concat(a, b, 's')) FROM ( VALUES (1, '2'), (1, '3'), (1, '4'), (2, '20'), (2, '30'), (2, '40') ) AS t(id, value) GROUP BY id",
                "values (1, 's2s3ss4ss'), (2, 's20s30ss40ss')");
        assertQueryFails("SELECT id, reduce_agg(value, array[id, value], (a, b) -> a || b, (a, b) -> a || b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id",
                ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
    }
}
