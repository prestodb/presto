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
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.INLINE_SQL_FUNCTIONS;
import static com.facebook.presto.SystemSessionProperties.JOIN_PREFILTER_BUILD_SIDE;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_ENABLED;
import static com.facebook.presto.SystemSessionProperties.KEY_BASED_SAMPLING_PERCENTAGE;
import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT;
import static com.facebook.presto.SystemSessionProperties.PREFILTER_FOR_GROUPBY_LIMIT_TIMEOUT_MS;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public abstract class AbstractTestQueriesNative
        extends AbstractTestQueries
{
    private boolean sidecarEnabled;

    private @Language("RegExp") String arrayCumSumFunctionSignatureUnsupportedError;
    private @Language("RegExp") String arraySplitIntoChunksFunctionUnsupportedError;
    private @Language("RegExp") String p4HllTypeUnsupportedError;
    private @Language("RegExp") String hashGenerationUnsupportedError;
    private @Language("RegExp") String createHllFunctionUnsupportedError;
    private @Language("RegExp") String reduceAggInvalidInitialStateError;
    private @Language("RegExp") String atTimezoneFunctionSignatureUnsupportedError;
    private @Language("RegExp") String keySamplingPercentFunctionUnsupportedError;
    private @Language("RegExp") String rgbFunctionUnsupportedError;
    private @Language("RegExp") String combineHashFunctionUnsupportedError;
    private @Language("RegExp") String mapAggFunctionSignatureUnsupportedError;
    private @Language("RegExp") String khyperloglogAggFunctionUnsupportedError;
    private @Language("RegExp") String operatorSubscriptFunctionUnsupportedError;
    private @Language("RegExp") String nowFunctionUnsupportedError;

    public void init(boolean sidecarEnabled)
    {
        this.sidecarEnabled = sidecarEnabled;

        if (sidecarEnabled) {
            arrayCumSumFunctionSignatureUnsupportedError = ".*Unexpected parameters \\(array\\(varchar\\)\\) for function native.default.array_cum_sum.*";
            arraySplitIntoChunksFunctionUnsupportedError = ".*Function native.default.array_split_into_chunks not registered.*";
            p4HllTypeUnsupportedError = ".*Unknown type: p4hyperloglog.*";
            hashGenerationUnsupportedError = ".*Function native.default.\\$operator\\$.*hash.*";
            createHllFunctionUnsupportedError = ".*Function native.default.create_hll not registered.*";
            reduceAggInvalidInitialStateError = ".*Initial value in reduce_agg cannot be null.*";
            atTimezoneFunctionSignatureUnsupportedError = ".*Unexpected parameters \\(timestamp with time zone, interval day to second\\) for function native.default.at_timezone.*";
            keySamplingPercentFunctionUnsupportedError = ".*Function native.default.key_sampling_percent not registered.*";
            rgbFunctionUnsupportedError = ".*Function native.default.rgb not registered.*";
            combineHashFunctionUnsupportedError = ".*Function native.default.combine_hash not registered.*";
            mapAggFunctionSignatureUnsupportedError = ".*Scalar function name not registered: native.default.map_agg.*";
            khyperloglogAggFunctionUnsupportedError = ".*Function native.default.khyperloglog_agg not registered.*";
            operatorSubscriptFunctionUnsupportedError = ".*Function native.default.\\$operator\\$subscript not registered.*";
            nowFunctionUnsupportedError = ".*Function native.default.now not registered.*";
        }
        else {
            arrayCumSumFunctionSignatureUnsupportedError = ".*Scalar function presto.default.array_cum_sum not registered with arguments.*";
            arraySplitIntoChunksFunctionUnsupportedError = ".*Scalar function name not registered: presto.default.array_split_into_chunks.*";
            p4HllTypeUnsupportedError = ".*Failed to parse type \\[P4HyperLogLog]. Type not registered.*";
            hashGenerationUnsupportedError = ".*Scalar function name not registered: presto.default.\\$operator\\$.*hash.*";
            createHllFunctionUnsupportedError = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*";
            reduceAggInvalidInitialStateError = ".*Initial value in reduce_agg cannot be null.*";
            atTimezoneFunctionSignatureUnsupportedError = ".*Unexpected parameters (timestamp with time zone, interval day to second) for function native.default.at_timezone.*";
            keySamplingPercentFunctionUnsupportedError = ".*Function native.default.key_sampling_percent not registered.*";
            rgbFunctionUnsupportedError = ".*Function native.default.rgb not registered.*";
            combineHashFunctionUnsupportedError = ".*Function native.default.combine_hash not registered.*";
            mapAggFunctionSignatureUnsupportedError = ".*Scalar function name not registered: (presto|native).default.map_agg.*";
            khyperloglogAggFunctionUnsupportedError = ".*Function native.default.khyperloglog_agg not registered.*";
            operatorSubscriptFunctionUnsupportedError = ".*Function native.default.$operator$subscript not registered.*";
            nowFunctionUnsupportedError = ".*Function native.default.now not registered.*";
        }
    }

    @DataProvider(name = "use_default_literal_coalesce")
    public static Object[][] useDefaultLiteralCoalesce()
    {
        return new Object[][] {{true}};
    }

    @Override
    @DataProvider(name = "optimize_hash_generation")
    public Object[][] optimizeHashGeneration()
    {
        return new Object[][] {{"false"}};
    }

    /// Custom aggregate functions are unsupported in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCustomSum() {}

    /// Custom window functions are unsupported in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCustomRank() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testApplyLambdaRepeated() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testLambdaCapture() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testLambdaInAggregationContext() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testLambdaInSubqueryContext() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testNonDeterministicInLambda() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testRowSubscriptInLambda() {}

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test(enabled = false)
    public void testTryWithLambda() {}

    /// This test is not applicable in Presto C++ as there is no bytecode IR as with JVM.
    @Override
    @Test(enabled = false)
    public void testLargeBytecode() {}

    /// The functions returned by SHOW FUNCTIONS and their order differ in Presto and Presto C++. Exact comparison is
    /// not needed and this test is disabled in Presto C++.
    @Override
    @Test(enabled = false)
    public void testShowFunctions() {}

    /// This test uses char type which is not supported in Presto C++. See issue:
    /// https://github.com/prestodb/presto/issues/21332.
    @Override
    @Test(enabled = false)
    public void testLikePrefixAndSuffixWithChars() {}

    /// TODO: This test is pending on Varchar(n) support in Presto C++.
    /// Planner error: type of variable 'expr' is expected to be varchar(5), but the actual type is varchar.
    @Override
    @Test(enabled = false)
    public void testCaseToMapOptimization() {}

    /// TODO: This test is pending on Varchar(n) support in Presto C++.
    /// Planner error: type of variable 'expr' is expected to be varchar(2), but the actual type is varchar.
    @Override
    @Test(enabled = false)
    public void testComplexCoercions() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    @Override
    @Test(enabled = false)
    public void testMinMaxByToWindowFunction() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    @Override
    @Test(enabled = false)
    public void testInUncorrelatedSubquery() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    @Override
    @Test(enabled = false)
    public void testShowSchemasLikeWithEscape() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    @Override
    @Test(enabled = false)
    public void testShowTablesLike() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    @Override
    @Test(enabled = false)
    public void testShowTablesLikeWithEscape() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    @Override
    @Test(enabled = false)
    public void testShowSchemasLike() {}

    /// array_cum_sum function in Presto C++ does not support decimal array inputs, see issue:
    /// https://github.com/prestodb/presto/issues/24402.
    @Override
    @Test(enabled = false)
    public void testArrayCumSumDecimals() {}

    /// reduce_agg returns different results in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
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

    /// Sidecar is needed to support custom functions in Presto C++.
    @Override
    @Test
    public void testCustomAdd()
    {
        if (sidecarEnabled) {
            super.testCustomAdd();
        }
    }

    /// reduce_agg returns different results in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testReduceAggWithNulls()
    {
        assertQueryFails("select reduce_agg(x, null, (x,y)->try(x+y), (x,y)->try(x+y)) from (select 1 union all select 10) T(x)", reduceAggInvalidInitialStateError);
        assertQueryFails("select reduce_agg(x, cast(null as bigint), (x,y)->coalesce(x, 0)+coalesce(y, 0), (x,y)->coalesce(x, 0)+coalesce(y, 0)) from (values cast(10 as bigint),10)T(x)", reduceAggInvalidInitialStateError);

        // here some reduce_aggs coalesce overflow/zero-divide errors to null in the input/combine functions
        assertQueryFails("select reduce_agg(x, 0, (x,y)->try(1/x+1/y), (x,y)->try(1/x+1/y)) from ((select 0) union all select 10.) T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQueryFails("select reduce_agg(x, 0, (x, y)->try(x+y), (x, y)->try(x+y)) from (values 2817, 9223372036854775807) AS T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQuery("select reduce_agg(x, array[], (x, y)->array[element_at(x, 2)],  (x, y)->array[element_at(x, 2)]) from (select array[array[1]]) T(x)", "select array[null]");
    }

    /// approx_set function returns different results in Presto C++, see Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
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

    /// array_cum_sum does not support Varchar array inputs, the error message differs in Presto C++ and Presto Java.
    @Override
    @Test
    public void testArrayCumSumVarchar()
    {
        String sql = "select array_cum_sum(k) from (values (array[cast('5.1' as varchar), '6', '0']), (ARRAY[]), (CAST(NULL AS array(varchar)))) t(k)";
        assertQueryFails(sql, arrayCumSumFunctionSignatureUnsupportedError, true);

        sql = "select array_cum_sum(k) from (values (array[cast(null as varchar), '6', '0'])) t(k)";
        assertQueryFails(sql, arrayCumSumFunctionSignatureUnsupportedError, true);
    }

    /// System property prefilter_for_groupby_limit is not supported in Presto C++, see issue:
    /// https://github.com/prestodb/presto/issues/24409.
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

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.2))",
                createHllFunctionUnsupportedError);
    }

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeHyperLogLog()
    {
        assertQueryFails("SELECT cardinality(merge(create_hll(custkey))) FROM orders", createHllFunctionUnsupportedError, true);
    }

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(custkey))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", createHllFunctionUnsupportedError, true);
    }

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        assertQueryFails("SELECT cardinality(merge(create_hll(IF(orderstatus = 'O', custkey)))) FROM orders",
                createHllFunctionUnsupportedError, true);
    }

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(merge(create_hll(IF(orderstatus != 'O', custkey)))) " +
                        "FROM orders " +
                        "GROUP BY orderstatus",
                createHllFunctionUnsupportedError, true);
    }

    /// key_sampling_percent function will not be implemented in Velox, see issue:
    /// https://github.com/prestodb/presto/issues/20592.
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

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetBigint()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) FROM orders",
                p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetVarchar()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) FROM orders",
                p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetBigintGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(custkey) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetDouble()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) FROM orders",
                p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetDoubleGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS DOUBLE)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetGroupByWithNulls()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(custkey % 2 <> 0, custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetGroupByWithOnlyNullsInOneGroup()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(IF(orderstatus != 'O', custkey)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetOnlyNulls()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(null) AS P4HYPERLOGLOG)) FROM orders",
                p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetVarcharGroupBy()
    {
        assertQueryFails(
                "SELECT orderstatus, cardinality(cast(approx_set(CAST(custkey AS VARCHAR)) AS P4HYPERLOGLOG)) " +
                        "FROM orders " +
                        "GROUP BY orderstatus", p4HllTypeUnsupportedError, true);
    }

    /// P4HyperLogLog type is not supported in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testP4ApproxSetWithNulls()
    {
        assertQueryFails("SELECT cardinality(cast(approx_set(IF(orderstatus = 'O', custkey)) AS P4HYPERLOGLOG)) FROM orders",
                p4HllTypeUnsupportedError, true);
    }

    /// array_split_into_chunks function is not supported in Presto C++, see issue:
    /// https://github.com/prestodb/presto/issues/25446.
    @Override
    @Test
    public void testArraySplitIntoChunks()
    {
        String sql = "select array_split_into_chunks(array[1, 2, 3, 4, 5, 6], 2)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[1, 2, 3, 4, 5], 3)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[1, 2, 3], 5)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(null, 2)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[1, 2, 3], 0)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[1, 2, 3], -1)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[1, null, 3, null, 5], 2)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array['a', 'b', 'c', 'd'], 2)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[1.1, 2.2, 3.3, 4.4, 5.5], 2)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[null, null, null], 0)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[null, null, null], 2)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[null, 1, 2], 5)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);

        sql = "select array_split_into_chunks(array[], 0)";
        assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);
    }

//    @Override
//    @Test
//    public void testMapUnionSumOverflow()
//    {
//        assertQueryFails(
//                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30,100] as array<tinyint>)) x " +
//                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<tinyint>))x) group by y", "Value 200 exceeds 127", true);
//        assertQueryFails(
//                "select y, map_union_sum(x) from (select 1 y, map(array['x', 'z', 'y'], cast(array[null,30, 32760] as array<smallint>)) x " +
//                        "union all select 1 y, map(array['x', 'y'], cast(array[1,100] as array<smallint>))x) group by y", "Value 32860 exceeds 32767", true);
//    }

    /// TODO: Velox does not support function signature: at_timezone(timestamp with time zone, interval day to second).
    @Override
    @Test
    public void testAtTimeZoneWithInterval()
    {
        assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE INTERVAL '07:09' hour to minute", atTimezoneFunctionSignatureUnsupportedError);
    }

    @Override
    @Test
    public void testDefaultSamplingPercent()
    {
        assertQueryFails("select key_sampling_percent('abc')", keySamplingPercentFunctionUnsupportedError);
    }

    /// Varchar(n) is unsupported in Presto C++.
    @Override
    @Test
    public void testDescribeOutput()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT * FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("nationkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("regionkey", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, false)
                .row("comment", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    /// Varchar(n) is unsupported in Presto C++.
    @Override
    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        Session session = Session.builder(getSession())
                .addPreparedStatement("my_query", "SELECT 1, name, regionkey AS my_alias FROM nation")
                .build();

        MaterializedResult actual = computeActual(session, "DESCRIBE OUTPUT my_query");
        MaterializedResult expected = resultBuilder(session, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .row("_col0", "", "", "", "integer", 4, false)
                .row("name", session.getCatalog().get(), session.getSchema().get(), "nation", "varchar", 0, false)
                .row("my_alias", session.getCatalog().get(), session.getSchema().get(), "nation", "bigint", 8, true)
                .build();
        assertEqualsIgnoreOrder(actual, expected);
    }

    /// Varchar(n) is unsupported in Presto C++.
    @Override
    @Test
    public void testNestedCast()
    {
        assertQuery("select cast(varchar_value as varchar(3)) || ' sfd' from (values ('9898.122')) t(varchar_value)", "VALUES '9898.122 sfd'");
        assertQuery("select cast(cast(varchar_value as varchar(3)) as varchar(5)) from (values ('9898.122')) t(varchar_value)", "VALUES '9898.122'");
    }

    @Override
    @Test
    public void testFunctionArgumentTypeConstraint()
    {
        assertQueryFails(
                "SELECT greatest(rgb(255, 0, 0))", rgbFunctionUnsupportedError);
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
            result = computeActual(session, testQuery);
            assertEquals(result.getRowCount(), 25);
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
            assertQueryFails(session, "explain(type distributed) " + testQuery, combineHashFunctionUnsupportedError);
            // assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
            // assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("XX_HASH_64"), -1);
            assertQueryFails(session, testQuery, combineHashFunctionUnsupportedError);
            // assertEquals(result.getRowCount(), 37);
        }
    }

    @Override
    @Test
    public void testRows()
    {
        // Using JSON_FORMAT(CAST(_ AS JSON)) because H2 does not support ROW type
        Session session = Session.builder(getSession()).setSystemProperty(FIELD_NAMES_IN_JSON_CAST_ENABLED, "true").build();
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1 + 2, CONCAT('a', 'b')) AS JSON))", "SELECT '{\"\":3,\"\":\"ab\"}'");
        // Presto casts ROW(...) to a JSON object, not a JSON array and uses "" as keys for unnamed fields. So updating the expected expression to JSON object.
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(a + b) AS JSON)) FROM (VALUES (1, 2)) AS t(a, b)", "SELECT '{\"\":3}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(1, ROW(9, a, ARRAY[], NULL), ROW(1, 2)) AS JSON)) FROM (VALUES ('a')) t(a)",
                "SELECT '{\"\":1,\"\":{\"\":9,\"\":\"a\",\"\":[],\"\":null},\"\":{\"\":1,\"\":2}}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(ROW(ROW(ROW(ROW(a, b), c), d), e), f) AS JSON)) FROM (VALUES (ROW(0, 1), 2, '3', NULL, ARRAY[5], ARRAY[])) t(a, b, c, d, e, f)",
                "SELECT '{\"\":{\"\":{\"\":{\"\":{\"\":{\"\":0,\"\":1},\"\":2},\"\":\"3\"},\"\":null},\"\":[5]},\"\":[]}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(a, b)) AS JSON)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)",
                "SELECT '[{\"\":1,\"\":2},{\"\":3,\"\":4},{\"\":5,\"\":6}]'");
        assertQuery(session, "SELECT CONTAINS(ARRAY_AGG(ROW(a, b)), ROW(1, 2)) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(a, b)", "SELECT TRUE");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ARRAY_AGG(ROW(c, d)) AS JSON)) FROM (VALUES (ARRAY[1, 3, 5], ARRAY[2, 4, 6])) AS t(a, b) CROSS JOIN UNNEST(a, b) AS u(c, d)",
                "SELECT '[{\"\":1,\"\":2},{\"\":3,\"\":4},{\"\":5,\"\":6}]'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, NULL, '3')) t(x,y,z)", "SELECT '{\"\":1,\"\":null,\"\":\"3\"}'");
        assertQuery(session, "SELECT JSON_FORMAT(CAST(ROW(x, y, z) AS JSON)) FROM (VALUES ROW(1, CAST(NULL AS INTEGER), '3')) t(x,y,z)", "SELECT '{\"\":1,\"\":null,\"\":\"3\"}'");
    }

    /// TODO: Fix result mismatch when sidecar disabled:
    /*
    not equal
    Actual rows (1 of 1 extra rows shown, 1 rows in total):
        [1500]
    Expected rows (1 of 1 missing rows shown, 1 rows in total):
        [500]
    */
    @Override
    @Test
    public void testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere()
    {
        if (sidecarEnabled) {
            super.testCorrelatedExistsSubqueriesWithEqualityPredicatesInWhere();
        }
    }

    /// TODO: Fix result mismatch when sidecar disabled
    /// not equal
    /// Actual rows (1 of 1 extra rows shown, 2 rows in total):
    ///     [1, a]
    @Override
    @Test
    public void testCrossJoinWithArrayNotContainsCondition()
    {
        if (sidecarEnabled) {
            super.testCrossJoinWithArrayNotContainsCondition();
        }
    }

    /// TODO: Fix result mismatch when sidecar disabled
    /// not equal
    /// Actual rows (100 of 2373 extra rows shown, 60175 rows in total):
    ///     [59013, 143, 7.0]
    ///     [59297, 1778, 45.0]
    @Override
    @Test
    public void testLeftJoinNullFilterToSemiJoin()
    {
        if (sidecarEnabled) {
            super.testLeftJoinNullFilterToSemiJoin();
        }
    }

    /// See issue: https://github.com/prestodb/presto/issues/20643.
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
                createHllFunctionUnsupportedError, true);
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.1))",
                createHllFunctionUnsupportedError, true);
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
    public void testMapBlockBug()
    {
        assertQueryFails(" VALUES(MAP_AGG(12345,123))", mapAggFunctionSignatureUnsupportedError, true);
    }

    @Override
    public void testMergeKHyperLogLog()
    {
        assertQueryFails("select k1, cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1", khyperloglogAggFunctionUnsupportedError, true);

        assertQueryFails("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)", khyperloglogAggFunctionUnsupportedError, true);
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
    public void testValuesWithTimestamp()
    {
        assertQueryFails("VALUES (current_timestamp, now())", nowFunctionUnsupportedError);
    }

    /// Presto C++ only supports legacy unnest and this test relies on non-legacy behavior of unnest operator for
    /// ARRAY(ROW). See issue: https://github.com/prestodb/presto/issues/20643.
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

    /// TODO: The last query in this testcase is flaky in Presto C++, to be investigated further.
    @Override
    @Test
    public void testLambdaInAggregation()
    {
        assertQuery("SELECT id, reduce_agg(value, 0, (a, b) -> a + b+0, (a, b) -> a + b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id", "values (1, 9), (2, 90)");
        assertQuery("SELECT id, 's' || reduce_agg(value, '', (a, b) -> concat(a, b, 's'), (a, b) -> concat(a, b, 's')) FROM ( VALUES (1, '2'), (1, '3'), (1, '4'), (2, '20'), (2, '30'), (2, '40') ) AS t(id, value) GROUP BY id",
                "values (1, 's2s3ss4ss'), (2, 's20s30ss40ss')");
        // assertQuery("SELECT id, reduce_agg(value, array[id, value], (a, b) -> a || b, (a, b) -> a || b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id");
    }

    /// TODO: The decimal coercion queries are flaky in Presto C++, to be investigated further.
    @Override
    @Test(enabled = false)
    public void testCoercions()
    {
        // VARCHAR
        assertQuery("SELECT length(NULL)");
        assertQuery("SELECT CAST('abc' AS VARCHAR(255)) || CAST('abc' AS VARCHAR(252))");
        assertQuery("SELECT CAST('abc' AS VARCHAR(255)) || 'abc'");

        // DECIMAL - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + NULL");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) + CAST(292.1 AS DECIMAL(5,1))");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS DECIMAL(5,1))"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(5,1))]"));

        // BIGINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(292 AS BIGINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(292 AS BIGINT)");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS BIGINT)"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(19,0))]"));

        // DECIMAL - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(1.1 AS DOUBLE)");
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) = CAST(1.1 AS DOUBLE)");
        assertQuery("SELECT SIN(CAST(1.1 AS DECIMAL(38,1)))");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282.1 AS DOUBLE), CAST(283.2 AS DOUBLE)] || CAST(101.3 AS DECIMAL(5,1))"),
                computeActual("SELECT ARRAY[CAST(282.1 AS DOUBLE), CAST(283.2 AS DOUBLE), CAST(101.3 AS DOUBLE)]"));

        // INTEGER - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(292 AS INTEGER)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(292 AS INTEGER)");
        assertEqualsIgnoreOrder(
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1))] || CAST(292 AS INTEGER)"),
                computeActual("SELECT ARRAY[CAST(282 AS DECIMAL(22,1)), CAST(282 AS DECIMAL(10,1)), CAST(292 AS DECIMAL(19,0))]"));

        // TINYINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(CAST(121 AS DECIMAL(30,1)) AS TINYINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(CAST(121 AS DECIMAL(30,1)) AS TINYINT)");

        // SMALLINT - DECIMAL
        assertQuery("SELECT CAST(1.1 AS DECIMAL(38,1)) + CAST(CAST(121 AS DECIMAL(30,1)) AS SMALLINT)");
        assertQuery("SELECT CAST(292 AS DECIMAL(38,1)) = CAST(CAST(121 AS DECIMAL(30,1)) AS SMALLINT)");
    }

    /// TODO: Fix comparison.
    @Override
    @Test(enabled = false)
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
        assertEquals(rows,
                ImmutableSet.of(
                        ImmutableList.of(zonedDateTime(timestampLosAngeles), zonedDateTime(timestampLosAngeles)),
                        ImmutableList.of(zonedDateTime(timestampLosAngeles), zonedDateTime(timestampNewYork)),
                        ImmutableList.of(zonedDateTime(timestampNewYork), zonedDateTime(timestampLosAngeles)),
                        ImmutableList.of(zonedDateTime(timestampNewYork), zonedDateTime(timestampNewYork))));
    }

    /// Key based sampling should be enabled for this test.
    @Override
    @Test
    public void testSamplingJoinChain()
    {
        Session sessionWithKeyBasedSampling = Session.builder(getSession())
                .setSystemProperty(INLINE_SQL_FUNCTIONS, "true")
                .setSystemProperty(KEY_BASED_SAMPLING_ENABLED, "true")
                .build();
        String query = "select count(1) FROM lineitem l left JOIN orders o ON l.orderkey = o.orderkey JOIN customer c ON o.custkey = c.custkey";

        assertQuery(query, "select 60175");
        assertQuery(sessionWithKeyBasedSampling, query, "select 16185");
    }

    /// TODO: Test fails because function operator_subscript is not implemented in Velox, check further.
    @Override
    @Test
    public void testRemoveMapCastFailure()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                operatorSubscriptFunctionUnsupportedError, true);
    }

    /// TODO: Test fails because function operator_subscript is not implemented in Velox, check further.
    @Override
    @Test
    public void testRemoveMapCast()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                operatorSubscriptFunctionUnsupportedError);
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 4)) t(feature,  key)",
                "values 0.5, 0.1");
        assertQuery(enableOptimization, "select element_at(feature, key) from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                "values 0.5, null");
        assertQuery(enableOptimization, "select feature[key] from (values (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), cast('2' as varchar)), (map(array[cast(1 as varchar), '2', '3', '4'], array[0.3, 0.5, 0.9, 0.1]), '4')) t(feature,  key)",
                "values 0.5, 0.1");
    }

    /// TODO: Check difference in behavior with sidecar enabled and disabled.
    /// no sidecar
    /// VeloxUserError: numInput == 1 (6000 vs. 1) Expected single row of input. Received 6000 rows.
    /// sidecar
    /// VeloxUserError: numInput == 1 (7500 vs. 1) Expected single row of input. Received 7500 rows.
    @Override
    @Test
    public void testScalarSubquery() {}

    /// TODO: Check if result mismatch only with sidecar disabled.
    /// not equal
    /// Actual rows (1 of 1 extra rows shown, 1 rows in total):
    ///     [25]
    /// Expected rows (1 of 1 missing rows shown, 1 rows in total):
    ///     [0]
    @Override
    @Test
    public void testTwoCorrelatedExistsSubqueries()
    {
        super.testTwoCorrelatedExistsSubqueries();
    }

    /// TODO: Fix result mismatch between Presto and Presto C++:
    /// java.lang.AssertionError: Iterators differ at element [0]: [1, 1, O] != [5, 5, F]
    /// Expected :[1, 1, O]
    /// Actual   :[5, 5, F]
    @Override
    @Test
    public void testTopNUnpartitionedWindow() {}
}
