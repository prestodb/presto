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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.JOIN_PREFILTER_BUILD_SIDE;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_PERCENTAGE;
import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertNotEquals;

public abstract class AbstractTestQueriesNative
        extends AbstractTestQueries
{
    private static final String UNSUPPORTED_CORRELATED_SUBQUERY_ERROR_MSG = "line .*: Given correlated subquery is not supported";
    private static final String P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR = ".*Failed to parse type \\[P4HyperLogLog]. Type not registered.*";
    private static final String KHLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR = ".*inferredType Failed to parse type \\[KHyperLogLog]. Type not registered.*";
    private static final String CHAR_TYPE_UNSUPPORTED_ERROR = ".*Failed to parse type \\[char\\(.*\\)]. syntax error, unexpected LPAREN, expecting WORD.*";
    private static final String HASH_GENERATION_UNSUPPORTED_ERROR = ".*Scalar function name not registered: presto.default.\\$operator\\$.*hash.*";
    private static final String ARRAY_COMPARISON_UNSUPPORTED_ERROR = ".*ARRAY comparison not supported for values that contain nulls.*";
    private static final String CREATE_HLL_FUNCTION_NOT_REGISTERED = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*";

    @DataProvider(name = "use_default_literal_coalesce")
    public static Object[][] useDefaultLiteralCoalesce()
    {
        return new Object[][] {{true}};
    }

    // These tests are disabled because they use the following functions that will not be supported in Presto C++:
    // custom_add, custom_sum, custom_rank, apply.
    @Override
    @Test(enabled = false)
    public void testCustomAdd() {}

    @Override
    @Test(enabled = false)
    public void testCustomSum() {}

    @Override
    @Test(enabled = false)
    public void testCustomRank() {}

    @Override
    @Test(enabled = false)
    public void testApplyLambdaRepeated() {}

    @Override
    @Test(enabled = false)
    public void testLambdaCapture() {}

    @Override
    @Test(enabled = false)
    public void testLambdaInAggregationContext() {}

    @Override
    @Test(enabled = false)
    public void testLambdaInSubqueryContext() {}

    @Override
    @Test(enabled = false)
    public void testNonDeterministicInLambda() {}

    @Override
    @Test(enabled = false)
    public void testRowSubscriptInLambda() {}

    @Override
    @Test(enabled = false)
    public void testTryWithLambda() {}

    // The following tests are disabled because Velox does not support optimized hash generation.
    @Override
    @Test(enabled = false)
    public void testDoubleDistinctPositiveAndNegativeZero(String optimizeHashGeneration) {}

    @Override
    @Test(enabled = false)
    public void testRealDistinctPositiveAndNegativeZero(String optimizeHashGeneration) {}

    @Override
    @Test(enabled = false)
    public void testRealJoinPositiveAndNegativeZero(String optimizeHashGeneration) {}

    @Override
    @Test(enabled = false)
    public void testDoubleJoinPositiveAndNegativeZero(String optimizeHashGeneration) {}

    // reduce_agg returns different results in Presto C++. See presto-docs/src/main/sphinx/presto_cpp/limitations.rst.
    @Override
    @Test
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
                        "       reduce_agg(y, ARRAY['x'], (a, b) -> a || b, (a, b) -> a || b)" +
                        "   ), " +
                        "   ''" +
                        ") " +
                        "FROM (VALUES (1, ARRAY['a']), (1, ARRAY['b']), (1, ARRAY['c']), (2, ARRAY['d']), (2, ARRAY['e']), (3, ARRAY['f'])) AS t(x, y) " +
                        "GROUP BY x",
                "VALUES (1, 'abcxxx'), (2, 'dexx'), (3, 'fx')");

        assertQuery("SELECT REDUCE_AGG((x,y), (0,0), (x, y)->(x[1],y[1]), (x,y)->(x[1],y[1]))[1] from (select 1 x, 2 y)", "select 0");
    }

    @Override
    @Test
    public void testReduceAggWithNulls()
    {
        assertQueryFails("select reduce_agg(x, null, (x,y)->try(x+y), (x,y)->try(x+y)) from (select 1 union all select 10) T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
        assertQueryFails("select reduce_agg(x, cast(null as bigint), (x,y)->coalesce(x, 0)+coalesce(y, 0), (x,y)->coalesce(x, 0)+coalesce(y, 0)) from (values cast(10 as bigint),10)T(x)", ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");

        // here some reduce_aggs coalesce overflow/zero-divide errors to null in the input/combine functions
        assertQueryFails("select reduce_agg(x, 0, (x,y)->try(1/x+1/y), (x,y)->try(1/x+1/y)) from ((select 0) union all select 10.) T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQueryFails("select reduce_agg(x, 0, (x, y)->try(x+y), (x, y)->try(x+y)) from (values 2817, 9223372036854775807) AS T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQuery("select reduce_agg(x, array[], (x, y)->array[element_at(x, 2)],  (x, y)->array[element_at(x, 2)]) from (select array[array[1]]) T(x)", "select array[null]");
    }

    // The following approx_set function tests are overriden since approx_set returns different results in Presto C++.
    @Override
    @Test
    public void testApproxSetBigint()
    {
        MaterializedResult actual = computeActual("SELECT cardinality(approx_set(custkey)) FROM orders");

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

        MaterializedResult expected = resultBuilder(getSession(), actual.getTypes())
                .row("O", null)
                .row("F", 1001L)
                .row("P", 304L)
                .build();

        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }

    @Override
    @Test
    public void testArrayCumSumDecimals()
    {
        String functionNotRegisteredError = ".*Scalar function presto.default.array_cum_sum not registered with arguments.*";
        String sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, 0]), (ARRAY[]), (CAST(NULL AS array(decimal)))) t(k)";
        assertQueryFails(sql, functionNotRegisteredError, true);

        sql = "select array_cum_sum(k) from (values (array[cast(5.1 as decimal(38, 1)), 6, null, 3]), (array[cast(null as decimal(38, 1)), 6, null, 3])) t(k)";
        assertQueryFails(sql, functionNotRegisteredError, true);
    }

    @Override
    @Test
    public void testArrayCumSumVarchar()
    {
        String sql = "select array_cum_sum(k) from (values (array[cast('5.1' as varchar), '6', '0']), (ARRAY[]), (CAST(NULL AS array(varchar)))) t(k)";
        assertQueryFails(sql, ".*Scalar function presto.default.array_cum_sum not registered with arguments.*", true);

        sql = "select array_cum_sum(k) from (values (array[cast(null as varchar), '6', '0'])) t(k)";
        assertQueryFails(sql, ".*Scalar function presto.default.array_cum_sum not registered with arguments: \\(ARRAY<VARCHAR>.*", true);
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

        assertQueryFails(prefilter, "select count(custkey), orderkey from orders where orderstatus='F' and orderkey < 50 group by orderkey limit 100", HASH_GENERATION_UNSUPPORTED_ERROR, true);
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
    public void testJoinPrefilter()
    {
        {
            // Orig
            String testQuery = "SELECT 1 from region join nation using(regionkey)";
            MaterializedResult result = computeActual("explain(type distributed) " + testQuery);
            assertEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(testQuery);
            assertEquals(result.getRowCount(), 25);

            // With feature
            Session session = Session.builder(getSession())
                    .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, String.valueOf(true))
                    .build();
            result = computeActual(session, "explain(type distributed) " + testQuery);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(session, testQuery);
            assertEquals(result.getRowCount(), 25);
        }

        {
            // Orig
            @Language("SQL") String testQuery = "SELECT 1 from region r join nation n on cast(r.regionkey as varchar) = cast(n.regionkey as varchar)";
            MaterializedResult result = computeActual("explain(type distributed) " + testQuery);
            assertEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(testQuery);
            assertEquals(result.getRowCount(), 25);

            // With feature
            Session session = Session.builder(getSession())
                    .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, String.valueOf(true))
                    .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, String.valueOf(false))
                    .build();
            result = computeActual(session, "explain(type distributed) " + testQuery);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("XX_HASH_64"), -1);
            assertQueryFails(session, testQuery, HASH_GENERATION_UNSUPPORTED_ERROR, true);
        }

        {
            // Orig
            String testQuery = "SELECT 1 from lineitem l join orders o on l.orderkey = o.orderkey and l.suppkey = o.custkey";
            MaterializedResult result = computeActual("explain(type distributed) " + testQuery);
            assertEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            result = computeActual(testQuery);
            assertEquals(result.getRowCount(), 37);

            // With feature
            Session session = Session.builder(getSession())
                    .setSystemProperty(JOIN_PREFILTER_BUILD_SIDE, String.valueOf(true))
                    .build();
            result = computeActual(session, "explain(type distributed) " + testQuery);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("XX_HASH_64"), -1);
            assertQueryFails(session, testQuery, HASH_GENERATION_UNSUPPORTED_ERROR, true);
        }
    }

    @Override
    @Test(expectedExceptions = {RuntimeException.class, PrestoException.class}, expectedExceptionsMessageRegExp = CREATE_HLL_FUNCTION_NOT_REGISTERED)
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        computeActual("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.2))");
    }

    @Override
    @Test
    public void testMergeHyperLogLog()
    {
        assertQueryFails("SELECT cardinality(merge(create_hll(custkey))) FROM orders", CREATE_HLL_FUNCTION_NOT_REGISTERED, true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", CREATE_HLL_FUNCTION_NOT_REGISTERED, true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        assertQueryFails("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders",
                CREATE_HLL_FUNCTION_NOT_REGISTERED, true);
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus",
                CREATE_HLL_FUNCTION_NOT_REGISTERED, true);
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
                P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetVarchar()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) FROM orders",
                P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetBigintGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetDouble()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) FROM orders",
                P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetDoubleGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetGroupByWithNulls()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(custkey % 2 <> 0, custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(orderstatus != 'O', custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetOnlyNulls()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(null) AS P4HYPERLOGLOG)) FROM orders",
                P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetVarcharGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testP4ApproxSetWithNulls()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(IF(orderstatus = 'O', custkey)) AS P4HYPERLOGLOG)) FROM orders",
                P4_HLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
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
    @Test
    public void testDuplicateUnnestRows()
    {
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3, r4, r5, r6, r7)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, r7, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);

        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                "Field not found: field_.*. Available fields are: field, field_.*", true);
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey) c FROM orders UNION ALL SELECT empty_approx_set())",
                CREATE_HLL_FUNCTION_NOT_REGISTERED, true);
    }

    @Override
    @Test
    public void testSetUnionWithNulls()
    {
        // all nulls should return empty array to match behavior of array_distinct(flatten(array_agg(x)))
        assertQueryFails(
                "select set_union(x) from (values null, null, null) as t(x)",
                "Unexpected type UNKNOWN", true);
        // nulls inside arrays should be captured while pure nulls should be ignored
        assertQueryFails(
                "select set_union(x) from (values null, array[null], null) as t(x)",
                "Unexpected type UNKNOWN", true);
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
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.1))",
                CREATE_HLL_FUNCTION_NOT_REGISTERED, true);
    }

    @Override
    @Test
    public void testCorrelatedNonAggregationScalarSubqueries()
    {
        String subqueryReturnedTooManyRows = ".*Scalar sub-query has returned multiple rows.*";

        assertQuery("SELECT (SELECT 1 WHERE a = 2) FROM (VALUES 1) t(a)", "SELECT null");
        assertQuery("SELECT (SELECT 2 WHERE a = 1) FROM (VALUES 1) t(a)", "SELECT 2");
        assertQueryFails(
                "SELECT (SELECT 2 FROM (VALUES 3, 4) WHERE a = 1) FROM (VALUES 1) t(a)",
                subqueryReturnedTooManyRows);

        // multiple subquery output projections
        // TODO: Check why native query runner doesn't throw an error for below queries.
        computeActual("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT 'bleh' FROM region WHERE regionkey > n.regionkey)");
        computeActual("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey > n.regionkey)");
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
        assertQuery("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey = n.regionkey)");

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
        assertQuery(
                "SELECT nationkey FROM nation n WHERE " +
                        "(SELECT regionkey * 2 FROM (SELECT regionkey FROM region r WHERE n.regionkey = r.regionkey)) > 6 " +
                        "ORDER BY 1 LIMIT 3",
                "VALUES 4, 10, 11"); // h2 didn't make it

        // with duplicated rows
        assertQuery(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 1, 1, 2, 3) t(a)",
                "VALUES 'ARGENTINA', 'ARGENTINA', 'BRAZIL', 'CANADA'"); // h2 didn't make it

        // returning null when nothing matched
        assertQuery(
                "SELECT (SELECT name FROM nation WHERE nationkey = a) FROM (VALUES 31) t(a)",
                "VALUES null");

        assertQuery(
                "SELECT (SELECT r.name FROM nation n, region r WHERE r.regionkey = n.regionkey AND n.nationkey = a) FROM (VALUES 1) t(a)",
                "VALUES 'AMERICA'");
    }

    @Override
    @Test
    public void testScalarSubquery()
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // TODO: Investigate error seen with disabled queries: 'expected types count (22) does not
        // match actual column count (16)'
        // aggregation
        computeActual("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT max(orderkey) FROM orders)");

        // no output
        assertQuery("SELECT * FROM lineitem WHERE orderkey = \n" +
                "(SELECT orderkey FROM orders WHERE 0=1)");

        // no output matching with null test
        computeActual("SELECT * FROM lineitem WHERE \n" +
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
        String multipleRowsErrorMsg = ".*Expected single row of input.*";
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
    @Test
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
    @Test
    public void testLikePrefixAndSuffixWithChars()
    {
        assertQueryFails("select x like 'abc%' from (values CAST ('abc' AS CHAR(3)), CAST ('def' AS CHAR(3)), CAST ('bcd' AS CHAR(3))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails("select x like '%abc%' from (values CAST ('xabcy' AS CHAR(5)), CAST ('abxabcdef' AS CHAR(9)), CAST ('bcd' AS CHAR(3)),  CAST ('xabcyabcz' AS CHAR(9))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails(
                "select x like '%abc' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4)), CAST ('xabc' AS CHAR(4)), CAST (' xabc' AS CHAR(5))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails("select x like '%ab_c' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails("select x like '%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails("select x like '%_%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails("select x like '%a%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
        assertQueryFails("select x like '%acd%xy%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", CHAR_TYPE_UNSUPPORTED_ERROR, true);
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
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1", KHLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);

        assertQueryFails("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)", KHLL_TIMESTAMP_TZ_TYPE_UNSUPPORTED_ERROR, true);
    }

    @Override
    @Test
    public void testRemoveMapCastFailure()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                "Cannot cast BIGINT.*to INTEGER. Overflow during arithmetic conversion", true);
    }

    @Override
    @Test
    public void testRemoveRedundantCastToVarcharInJoinClause()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN, "true")
                .build();
        // TODO: Check why results do not match for disabled queries.
        // Trigger optimization
        computeActual("select * from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        computeActual(session, "select *, cast(o.custkey as varchar), cast(c.custkey as varchar) from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)");
        assertQuery(session, "select r.custkey, r.orderkey, r.name, n.nationkey from (select o.custkey, o.orderkey, c.name from orders o join customer c on cast(o.custkey as varchar) = cast(c.custkey as varchar)) r, nation n");
        // Do not trigger optimization
        assertQuery(session, "select * from customer c join orders o on cast(acctbal as varchar) = cast(totalprice as varchar)");
    }

    @Override
    @Test
    public void testSameAggregationWithAndWithoutFilter()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "true")
                .build();
        Session disableOptimization = Session.builder(getSession())
                .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "false")
                .build();

        // TODO: Check why results do not match with MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER enabled.
        // Disabled tests to be enabled after fixing this issue.
        computeActual(getQueryRunner(), enableOptimization, "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey");
        assertQuery(enableOptimization, "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation", "values (15,25)");
        assertQuery(enableOptimization, "select count(1), count(1) filter (where k > 5) from (values 1, null, 3, 5, null, 8, 10) t(k)", "values (7, 2)");

        String sql = "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey";
        // MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
        // MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);
        // assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

        sql = "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation";
        // resultWithOptimization = computeActual(enableOptimization, sql);
        // resultWithoutOptimization = computeActual(disableOptimization, sql);
        // assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

        sql = "select partkey, sum(quantity), sum(quantity) filter (where discount > 0.1) from lineitem group by grouping sets((), (partkey))";
        MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
        MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);
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
                ARRAY_COMPARISON_UNSUPPORTED_ERROR, true);
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
                ARRAY_COMPARISON_UNSUPPORTED_ERROR, true);
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
    public void testLambdaInAggregation()
    {
        assertQuery("SELECT id, reduce_agg(value, 0, (a, b) -> a + b+0, (a, b) -> a + b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id", "values (1, 9), (2, 90)");
        assertQuery("SELECT id, 's' || reduce_agg(value, '', (a, b) -> concat(a, b, 's'), (a, b) -> concat(a, b, 's')) FROM ( VALUES (1, '2'), (1, '3'), (1, '4'), (2, '20'), (2, '30'), (2, '40') ) AS t(id, value) GROUP BY id",
                "values (1, 's2s3ss4ss'), (2, 's20s30ss40ss')");
        assertQueryFails("SELECT id, reduce_agg(value, array[id, value], (a, b) -> a || b, (a, b) -> a || b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id",
                ".*REDUCE_AGG only supports non-NULL literal as the initial value.*");
    }

    @Override
    @Test
    public void testPreserveAssignmentsInJoin()
    {
        // The following two timestamps represent the same point in time but with different time zones
        String timestampLosAngeles = "2001-08-22 03:04:05.321 America/Los_Angeles";
        String timestampNewYork = "2001-08-22 06:04:05.321 America/New_York";
        Set<List<Object>> rows = computeActual("WITH source AS (" +
                "SELECT * FROM (" +
                "    VALUES" +
                "        (TIMESTAMP '" + timestampLosAngeles + "')," +
                "        (TIMESTAMP '" + timestampNewYork + "')" +
                ") AS tbl (tstz)" +
                ")" +
                "SELECT * FROM source a JOIN source b ON a.tstz = b.tstz").getMaterializedRows().stream()
                .map(MaterializedRow::getFields)
                .collect(toImmutableSet());
        // TODO: Enable assertion after fixing result mismatch
        /* Assert.assertEquals(rows,
                ImmutableSet.of(
                        ImmutableList.of(zonedDateTime(timestampLosAngeles), zonedDateTime(timestampLosAngeles)),
                        ImmutableList.of(zonedDateTime(timestampLosAngeles), zonedDateTime(timestampNewYork)),
                        ImmutableList.of(zonedDateTime(timestampNewYork), zonedDateTime(timestampLosAngeles)),
                        ImmutableList.of(zonedDateTime(timestampNewYork), zonedDateTime(timestampNewYork)))); */
    }
}
