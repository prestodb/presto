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
package com.facebook.presto.nativetests;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueries;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.EXPRESSION_OPTIMIZER_NAME;
import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.JOIN_PREFILTER_BUILD_SIDE;
import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.SystemSessionProperties.REMOVE_REDUNDANT_CAST_TO_VARCHAR_IN_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_MIN_MAX_BY_TO_TOP_N;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public abstract class AbstractTestQueriesNative
        extends AbstractTestQueries
{
    private boolean sidecarEnabled;

    private @Language("RegExp") String applyNotRegisteredError;
    private @Language("RegExp") String createHllFunctionUnsupportedError;
    private @Language("RegExp") String charTypeUnsupportedError;
    private @Language("RegExp") String unnestRowsInvalidFieldError;

    public void init(boolean sidecarEnabled)
    {
        this.sidecarEnabled = sidecarEnabled;

        if (sidecarEnabled) {
            applyNotRegisteredError = ".*Function native.default.apply not registered*";
            charTypeUnsupportedError = ".*Unknown type: char.*";
            createHllFunctionUnsupportedError = ".*Function native.default.create_hll not registered.*";
            unnestRowsInvalidFieldError = "Field not found: field(?:_\\d+)?. Available fields are: field.*";
        }
        else {
            applyNotRegisteredError = ".*Scalar function name not registered: presto.default.apply.*";
            charTypeUnsupportedError = ".*Failed to parse type \\[char\\(.*\\)].*";
            createHllFunctionUnsupportedError = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*";
            unnestRowsInvalidFieldError = "Field not found: field(?:_\\d+)?. Available fields are: field.*";
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

    /// This test is not applicable in Presto C++ since there is no bytecode IR as with JVM.
    @Override
    @Test(enabled = false)
    public void testLargeBytecode() {}

    /// TODO: The decimal coercion queries are flaky in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCoercions() {}

    /// TODO: Enabling session property merge_aggregations_with_and_without_filter in Presto C++ clusters without the
    /// sidecar results in incorrect results. See issue https://github.com/prestodb/presto/issues/26323.
    @Override
    @Test
    public void testSameAggregationWithAndWithoutFilter()
    {
        if (sidecarEnabled) {
            super.testSameAggregationWithAndWithoutFilter();
        }
        else {
            Session enableOptimization = Session.builder(getSession())
                    .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "true")
                    .build();
            Session disableOptimization = Session.builder(getSession())
                    .setSystemProperty(MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER, "false")
                    .build();

            computeActual(getQueryRunner(), enableOptimization, "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey");
            assertQuery(enableOptimization, "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation", "values (15,25)");
            assertQuery(enableOptimization, "select count(1), count(1) filter (where k > 5) from (values 1, null, 3, 5, null, 8, 10) t(k)", "values (7, 2)");

            String sql = "select regionkey, count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation group by regionkey";
            // MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
            // MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);
            // assertEqualsIgnoreOrder(resultWithOptimization, resultWithoutOptimization);

            sql = "select count(name) filter (where name like '%N%') n_nations, count(name) all_nations from nation";
            MaterializedResult resultWithOptimization = computeActual(enableOptimization, sql);
            MaterializedResult resultWithoutOptimization = computeActual(disableOptimization, sql);
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
    }

    /// TODO: The last query in this testcase is flaky in Presto C++.
    @Override
    @Test
    public void testLambdaInAggregation()
    {
        assertQuery("SELECT id, reduce_agg(value, 0, (a, b) -> a + b+0, (a, b) -> a + b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id", "values (1, 9), (2, 90)");
        assertQuery("SELECT id, 's' || reduce_agg(value, '', (a, b) -> concat(a, b, 's'), (a, b) -> concat(a, b, 's')) FROM ( VALUES (1, '2'), (1, '3'), (1, '4'), (2, '20'), (2, '30'), (2, '40') ) AS t(id, value) GROUP BY id",
                "values (1, 's2s3ss4ss'), (2, 's20s30ss40ss')");
        // assertQuery("SELECT id, reduce_agg(value, array[id, value], (a, b) -> a || b, (a, b) -> a || b) FROM ( VALUES (1, 2), (1, 3), (1, 4), (2, 20), (2, 30), (2, 40) ) AS t(id, value) GROUP BY id");
    }

    /// TODO: Check why Presto C++ doesn't throw an error for certain queries.
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
        // TODO: Check why Presto C++ doesn't throw an error for below queries.
        /*assertQueryFails(
                "SELECT name FROM nation n WHERE 'AFRICA' = (SELECT 'bleh' FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);
        computeActual("SELECT name FROM nation n WHERE 'AFRICA' = (SELECT name FROM region WHERE regionkey > n.regionkey)");
        assertQueryFails(
                "SELECT name FROM nation n WHERE 1 = (SELECT 1 FROM region WHERE regionkey > n.regionkey)",
                subqueryReturnedTooManyRows);*/

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

    /// Sidecar is needed to support custom functions in Presto C++.
    @Override
    @Test
    public void testCustomAdd()
    {
        if (sidecarEnabled) {
            super.testCustomAdd();
        }
    }

    /// Sidecar is needed to support custom functions in Presto C++.
    /// TODO: Custom aggregate functions are currently unsupported in Presto C++.
    @Override
    @Test
    public void testCustomSum()
    {
        if (sidecarEnabled) {
            @Language("SQL") String sql = "SELECT orderstatus, custom_sum(orderkey) FROM orders GROUP BY orderstatus";
            assertQueryFails(sql, ".*Function native.default.custom_sum not registered.*");
        }
    }

    /// Sidecar is needed to support custom functions in Presto C++.
    /// TODO: Custom window functions are currently unsupported in Presto C++.
    @Override
    @Test
    public void testCustomRank()
    {
        if (sidecarEnabled) {
            @Language("SQL") String sql = "" +
                    "SELECT orderstatus, clerk, sales\n" +
                    ", custom_rank() OVER (PARTITION BY orderstatus ORDER BY sales DESC) rnk\n" +
                    "FROM (\n" +
                    "  SELECT orderstatus, clerk, sum(totalprice) sales\n" +
                    "  FROM orders\n" +
                    "  GROUP BY orderstatus, clerk\n" +
                    ")\n" +
                    "ORDER BY orderstatus, clerk";

            assertQueryFails(sql, ".*Function native.default.custom_rank not registered.*");
        }
    }

    /// TODO: Map union sum should support maps with decimal values, see issue:
    /// https://github.com/prestodb/presto/issues/26659.
    @Override
    @Test
    public void testInvalidMapUnionSum()
    {
        if (sidecarEnabled) {
            assertQueryFails(
                    "SELECT map_union_sum(x) from (select cast(MAP() as map<varchar, varchar>) x)",
                    ".*line 1:8: Unexpected parameters \\(map\\(varchar,varchar\\)\\) for function native.default.map_union_sum. Expected.*");
            assertQuerySucceeds("SELECT map_union_sum(x) from (select cast(MAP() as map<varchar, decimal(10,2)>) x)");
        }
        else {
            super.testInvalidMapUnionSum();
        }
    }

    /// This test uses char type which is not supported in Presto C++. See issue:
    /// https://github.com/prestodb/presto/issues/21332.
    @Override
    @Test
    public void testLikePrefixAndSuffixWithChars()
    {
        assertQueryFails("select x like 'abc%' from (values CAST ('abc' AS CHAR(3)), CAST ('def' AS CHAR(3)), CAST ('bcd' AS CHAR(3))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%abc%' from (values CAST ('xabcy' AS CHAR(5)), CAST ('abxabcdef' AS CHAR(9)), CAST ('bcd' AS CHAR(3)),  CAST ('xabcyabcz' AS CHAR(9))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%abc' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4)), CAST ('xabc' AS CHAR(4)), CAST (' xabc' AS CHAR(5))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%ab_c' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%_%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%a%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", charTypeUnsupportedError);
        assertQueryFails("select x like '%acd%xy%' from (values CAST('xa bc' AS CHAR(5)), CAST ('xabcy' AS CHAR(5)), CAST ('abcd' AS CHAR(4))) T(x)", charTypeUnsupportedError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testApplyLambdaRepeated()
    {
        assertQueryFails("SELECT x + x FROM (SELECT apply(a, i -> i * i) x FROM (VALUES 3) t(a))", applyNotRegisteredError);
        assertQueryFails("SELECT apply(a, i -> i * i) + apply(a, i -> i * i) FROM (VALUES 3) t(a)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(a, i -> i * i), apply(a, i -> i * i) FROM (VALUES 3) t(a)", applyNotRegisteredError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testLambdaCapture()
    {
        // Test for lambda expression without capture can be found in TestLambdaExpression

        assertQueryFails("SELECT apply(0, x -> x + c1) FROM (VALUES 1) t(c1)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(0, x -> x + t.c1) FROM (VALUES 1) t(c1)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(c1 + 10, x -> apply(x + 100, y -> t.c1)) FROM (VALUES 1) t(c1)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) u(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> r.x) FROM (VALUES 1) r(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) u(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), r -> apply(3, y -> y + r.x)) FROM (VALUES 'a') r(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(CAST(ROW(10) AS ROW(x INTEGER)), z -> apply(3, y -> y + r.x)) FROM (VALUES 1) r(x)", applyNotRegisteredError);

        // reference lambda variable of the not-immediately-enclosing lambda
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)", applyNotRegisteredError);

        // in join post-filter
        assertQueryFails("SELECT * FROM (VALUES true) t(x) left JOIN (VALUES 1001) t2(y) ON (apply(false, z -> apply(false, y -> x)))", applyNotRegisteredError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testLambdaInAggregationContext()
    {
        assertQueryFails("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", applyNotRegisteredError);
        assertQueryFails("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) GROUP BY x", applyNotRegisteredError);
        assertQueryFails("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y", applyNotRegisteredError);

        assertQueryFails("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y", applyNotRegisteredError);
        assertQueryFails("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)", applyNotRegisteredError);
        // nested lambda expression uses the same variable name
        assertQueryFails("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)", applyNotRegisteredError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testLambdaInSubqueryContext()
    {
        assertQueryFails("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply((SELECT 10), i -> i * i)", applyNotRegisteredError);

        // with capture
        assertQueryFails("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)", applyNotRegisteredError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testNonDeterministicInLambda()
    {
        assertQueryFails("SELECT apply(1, x -> x + rand()) FROM orders LIMIT 10", applyNotRegisteredError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testRowSubscriptInLambda()
    {
        assertQueryFails("SELECT apply(ROW (1, 2), r -> r[2])", applyNotRegisteredError);
    }

    /// Queries in this testcase use the apply function, which is used to test lambda expressions, and is currently
    /// unsupported in Presto C++. See issue: https://github.com/prestodb/presto/issues/20741.
    @Override
    @Test
    public void testTryWithLambda()
    {
        assertQueryFails("SELECT TRY(apply(5, x -> x + 1) / 0)", applyNotRegisteredError);
        assertQueryFails("SELECT TRY(apply(5 + RANDOM(1), x -> x + 1) / 0)", applyNotRegisteredError);
        assertQueryFails("SELECT apply(5 + RANDOM(1), x -> x + TRY(1 / 0))", applyNotRegisteredError);
    }

    /// The functions are fetched from sidecar when it is enabled so the output of SHOW FUNCTIONS and the order of
    /// functions differ.
    @Override
    @Test
    public void testShowFunctions()
    {
        if (sidecarEnabled) {
            MaterializedResult result = computeActual("SHOW FUNCTIONS");
            ImmutableMultimap<String, MaterializedRow> functions = Multimaps.index(result.getMaterializedRows(), input -> {
                assertEquals(input.getFieldCount(), 10);
                return (String) input.getField(0);
            });

            assertTrue(functions.containsKey("avg"), "Expected function names " + functions + " to contain 'avg'");
            assertEquals(functions.get("avg").asList().size(), 7);
            assertEquals(functions.get("avg").asList().get(0).getField(1), "decimal(a_precision,a_scale)");
            assertEquals(functions.get("avg").asList().get(0).getField(2), "decimal(a_precision,a_scale)");
            assertEquals(functions.get("avg").asList().get(0).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(1).getField(1), "double");
            assertEquals(functions.get("avg").asList().get(1).getField(2), "bigint");
            assertEquals(functions.get("avg").asList().get(1).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(2).getField(1), "double");
            assertEquals(functions.get("avg").asList().get(2).getField(2), "double");
            assertEquals(functions.get("avg").asList().get(2).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(3).getField(1), "double");
            assertEquals(functions.get("avg").asList().get(3).getField(2), "integer");
            assertEquals(functions.get("avg").asList().get(3).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(4).getField(1), "double");
            assertEquals(functions.get("avg").asList().get(4).getField(2), "smallint");
            assertEquals(functions.get("avg").asList().get(4).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(4).getField(1), "double");
            assertEquals(functions.get("avg").asList().get(4).getField(2), "smallint");
            assertEquals(functions.get("avg").asList().get(4).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(5).getField(1), "interval day to second");
            assertEquals(functions.get("avg").asList().get(5).getField(2), "interval day to second");
            assertEquals(functions.get("avg").asList().get(5).getField(3), "aggregate");
            assertEquals(functions.get("avg").asList().get(6).getField(1), "real");
            assertEquals(functions.get("avg").asList().get(6).getField(2), "real");
            assertEquals(functions.get("avg").asList().get(6).getField(3), "aggregate");

            assertTrue(functions.containsKey("abs"), "Expected function names " + functions + " to contain 'abs'");
            assertEquals(functions.get("abs").asList().get(0).getField(3), "scalar");
            assertEquals(functions.get("abs").asList().get(0).getField(4), true);
            assertEquals(functions.get("abs").asList().get(0).getField(6), false);
            assertEquals(functions.get("abs").asList().get(0).getField(7), false);
            assertEquals(functions.get("abs").asList().get(0).getField(8), false);
            assertEquals(functions.get("abs").asList().get(0).getField(9), "cpp");

            assertTrue(functions.containsKey("rand"), "Expected function names " + functions + " to contain 'rand'");
            assertEquals(functions.get("rand").asList().get(0).getField(3), "scalar");
            assertEquals(functions.get("rand").asList().get(0).getField(4), false);
            assertEquals(functions.get("rand").asList().get(0).getField(6), false);
            assertEquals(functions.get("rand").asList().get(0).getField(7), false);
            assertEquals(functions.get("rand").asList().get(0).getField(8), false);
            assertEquals(functions.get("rand").asList().get(0).getField(9), "cpp");

            assertTrue(functions.containsKey("rank"), "Expected function names " + functions + " to contain 'rank'");
            assertEquals(functions.get("rank").asList().get(0).getField(3), "window");
            assertEquals(functions.get("rank").asList().get(0).getField(4), true);
            assertEquals(functions.get("rank").asList().get(0).getField(6), false);
            assertEquals(functions.get("rank").asList().get(0).getField(7), false);
            assertEquals(functions.get("rank").asList().get(0).getField(8), false);
            assertEquals(functions.get("rank").asList().get(0).getField(9), "cpp");

            assertTrue(functions.containsKey("greatest"), "Expected function names " + functions + " to contain 'greatest'");
            assertEquals(functions.get("greatest").asList().get(0).getField(3), "scalar");
            assertEquals(functions.get("greatest").asList().get(0).getField(4), true);
            assertEquals(functions.get("greatest").asList().get(0).getField(6), true);
            assertEquals(functions.get("greatest").asList().get(0).getField(7), false);
            assertEquals(functions.get("greatest").asList().get(0).getField(8), false);
            assertEquals(functions.get("greatest").asList().get(0).getField(9), "cpp");

            assertTrue(functions.containsKey("split_part"), "Expected function names " + functions + " to contain 'split_part'");
            assertEquals(functions.get("split_part").asList().get(0).getField(1), "varchar");
            assertEquals(functions.get("split_part").asList().get(0).getField(2), "varchar, varchar, bigint");
            assertEquals(functions.get("split_part").asList().get(0).getField(3), "scalar");
            assertEquals(functions.get("split_part").asList().get(0).getField(4), true);
            assertEquals(functions.get("split_part").asList().get(0).getField(6), false);
            assertEquals(functions.get("split_part").asList().get(0).getField(7), false);
            assertEquals(functions.get("split_part").asList().get(0).getField(8), false);
            assertEquals(functions.get("split_part").asList().get(0).getField(9), "cpp");

            assertTrue(functions.containsKey("like"), "Expected function names " + functions + " to contain 'like'");
        }
        else {
            super.testShowFunctions();
        }
    }

    /// Custom session properties and catalog properties are not supported by native sidecar. Native execution only
    /// system session properties should also be excluded from the result of SHOW SESSION when sidecar is enabled.
    @Override
    @Test
    public void testShowSession()
    {
        if (sidecarEnabled) {
            Session session = new Session(
                    getSession().getQueryId(),
                    java.util.Optional.empty(),
                    getSession().isClientTransactionSupport(),
                    getSession().getIdentity(),
                    getSession().getSource(),
                    getSession().getCatalog(),
                    getSession().getSchema(),
                    getSession().getTraceToken(),
                    getSession().getTimeZoneKey(),
                    getSession().getLocale(),
                    getSession().getRemoteUserAddress(),
                    getSession().getUserAgent(),
                    getSession().getClientInfo(),
                    getSession().getClientTags(),
                    getSession().getResourceEstimates(),
                    getSession().getStartTime(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    getQueryRunner().getMetadata().getSessionPropertyManager(),
                    getSession().getPreparedStatements(),
                    ImmutableMap.of(),
                    getSession().getTracer(),
                    getSession().getWarningCollector(),
                    getSession().getRuntimeStats(),
                    getSession().getQueryType());

            String nativeSystemPropertiesRegex = "Native Execution only.*";
            MaterializedResult result = computeActual(session, "SHOW SESSION");
            List<MaterializedRow> actualRows = result.getMaterializedRows();

            // Ensure there are no duplicates in the native system session properties reported by the sidecar.
            List<MaterializedRow> filteredRows = actualRows.stream()
                    .filter(row -> Pattern.matches(nativeSystemPropertiesRegex, row.getFields().get(4).toString()))
                    .collect(Collectors.toList());
            ImmutableMap<String, MaterializedRow> properties = Maps.uniqueIndex(filteredRows, input -> {
                assertEquals(input.getFieldCount(), 5);
                return (String) input.getField(0);
            });
            assertEquals(properties.size(), filteredRows.size(), "Duplicate native system session properties found.");

            try {
                computeActual(session, "SHOW SESSION LIKE 't$_%' ESCAPE ''");
                fail();
            }
            catch (Exception e) {
                assertTrue(e.getMessage().contains("Escape string must be a single character"));
            }

            try {
                computeActual(session, "SHOW SESSION LIKE 't$_%' ESCAPE '$$'");
                fail();
            }
            catch (Exception e) {
                assertTrue(e.getMessage().contains("Escape string must be a single character"));
            }
        }
        else {
            super.testShowSession();
        }
    }

    /// The integer overflow error message differs in Presto and Velox.
    @Override
    @Test
    public void testRemoveMapCastFailure()
    {
        Session enableOptimization = Session.builder(getSession())
                .setSystemProperty(REMOVE_MAP_CAST, "true")
                .build();
        assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                ".*Cannot cast BIGINT '400000000000' to INTEGER. Overflow during arithmetic conversion.*");
    }

    /// TODO: Velox does not support function signature: at_timezone(timestamp with time zone, interval day to second).
    /// See issue: https://github.com/prestodb/presto/issues/26666.
    @Override
    @Test
    public void testAtTimeZoneWithInterval()
    {
        if (sidecarEnabled) {
            @Language("RegExp") String atTimezoneFunctionSignatureUnsupportedError = ".*Unexpected parameters \\(timestamp with time zone, interval day to second\\) for function native.default.at_timezone.*";
            assertQueryFails("SELECT TIMESTAMP '2012-10-31 01:00' AT TIME ZONE INTERVAL '07:09' hour to minute", atTimezoneFunctionSignatureUnsupportedError);
        }
        else {
            super.testAtTimeZoneWithInterval();
        }
    }

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

    /// reduce_agg returns different results in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test
    public void testReduceAggWithNulls()
    {
        @Language("RegExp") String reduceAggInvalidInitialStateError = sidecarEnabled ? ".*Initial value in reduce_agg cannot be null.*" : ".*REDUCE_AGG only supports non-NULL literal as the initial value.*";
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

    /// array_cum_sum does not support Varchar array inputs, the error message differs in Presto and Velox.
    @Override
    @Test
    public void testArrayCumSumVarchar()
    {
        @Language("RegExp") String arrayCumSumFunctionSignatureUnsupportedError = sidecarEnabled ? ".*Unexpected parameters \\(array\\(varchar\\)\\) for function native.default.array_cum_sum.*" : ".*Scalar function presto.default.array_cum_sum not registered with arguments.*";

        String sql = "select array_cum_sum(k) from (values (array[cast('5.1' as varchar), '6', '0']), (ARRAY[]), (CAST(NULL AS array(varchar)))) t(k)";
        assertQueryFails(sql, arrayCumSumFunctionSignatureUnsupportedError, true);

        sql = "select array_cum_sum(k) from (values (array[cast(null as varchar), '6', '0'])) t(k)";
        assertQueryFails(sql, arrayCumSumFunctionSignatureUnsupportedError, true);
    }

    /// The error message for the case where subquery returns multiple rows differs in Presto and Velox.
    @Override
    @Test
    public void testScalarSubquery()
    {
        // nested
        assertQuery("SELECT (SELECT (SELECT (SELECT 1)))");

        // aggregation
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

    /// TODO: Native expression optimizer is required to support system property join_prefilter_build_side in Presto
    /// C++ with sidecar. Pending on https://github.com/prestodb/presto/pull/26475.
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

            if (sidecarEnabled) {
                @Language("RegExp") String combineHashFunctionUnsupportedError = ".*Function native.default.combine_hash not registered.*";
                assertQueryFails(session, "explain(type distributed) " + testQuery, combineHashFunctionUnsupportedError);
                assertQueryFails(session, testQuery, combineHashFunctionUnsupportedError);
            }
            else {
                result = computeActual(session, "explain(type distributed) " + testQuery);
                assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("SemiJoin"), -1);
                assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("XX_HASH_64"), -1);
                result = computeActual(session, testQuery);
                assertEquals(result.getRowCount(), 37);
            }
        }
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

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey) c FROM orders UNION ALL SELECT empty_approx_set())",
                createHllFunctionUnsupportedError, true);
    }

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        assertQueryFails("SELECT cardinality(merge(c)) FROM (SELECT create_hll(custkey, 0.1) c FROM orders UNION ALL SELECT empty_approx_set(0.1))",
                createHllFunctionUnsupportedError, true);
    }

    /// Color functions are not supported in Presto C++.
    @Override
    @Test
    public void testFunctionArgumentTypeConstraint()
    {
        @Language("RegExp") String errorMessage = sidecarEnabled ? ".*Function native.default.rgb not registered.*" : ".*Unexpected parameters \\(color\\) for function greatest.*";
        assertQueryFails("SELECT greatest(rgb(255, 0, 0))", errorMessage);
    }

    /// The output JSON formatted string is different in Presto C++.
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

    /// TODO: Presto C++ only supports legacy unnest and this test relies on non-legacy behavior of unnest operator for
    /// ARRAY(ROW). See issue: https://github.com/prestodb/presto/issues/20643.
    @Override
    @Test
    public void testDuplicateUnnestRows()
    {
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                unnestRowsInvalidFieldError, true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) AS r(r1, r2, r3, r4, r5, r6, r7)",
                unnestRowsInvalidFieldError, true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                unnestRowsInvalidFieldError, true);
        assertQueryFails("SELECT * from (select * FROM (values 1) as t(k)) CROSS JOIN unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)], ARRAY[row(10, 13, 15), row(23, 25, 20)]) WITH ORDINALITY AS r(r1, r2, r3, r4, r5, r6, r7, ord)",
                unnestRowsInvalidFieldError, true);

        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) AS r(r1, r2, r3, r4)",
                unnestRowsInvalidFieldError, true);
        assertQueryFails("SELECT * from unnest(ARRAY[row(2, 3), row(3, 5)], ARRAY[row(2, 3), row(3, 5)]) WITH ORDINALITY AS r(r1, r2, r3, r4, ord)",
                unnestRowsInvalidFieldError, true);
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
                unnestRowsInvalidFieldError, true);
    }

    /// The error message for invalid map_agg function differs in Presto and Velox.
    @Override
    @Test(enabled = false)
    public void testMapBlockBug()
    {
        Session session = Session.builder(getSession())
                .setSystemProperty(EXPRESSION_OPTIMIZER_NAME, "native")
                .build();
        @Language("RegExp") String mapAggFunctionSignatureUnsupportedError = sidecarEnabled ? ".*Scalar function name not registered: native.default.map_agg.*" : ".*Scalar function name not registered: presto.default.map_agg.*";
        if (sidecarEnabled) {
            assertQueryFails(session, " VALUES(MAP_AGG(12345,123))", mapAggFunctionSignatureUnsupportedError, true);
        }
        else {
            assertQueryFails(" VALUES(MAP_AGG(12345,123))", mapAggFunctionSignatureUnsupportedError, true);
        }
    }

    /// TODO: Native expression optimizer should be enabled for the following tests with sidecar enabled. Once
    ///  parameterized Varchar type is supported in Velox, native expression optimizer will be enabled by default with
    ///  sidecar for all tests.
    @Override
    @Test
    public void testValuesWithTimestamp()
    {
        if (sidecarEnabled) {
            // Plan validation with NativePlanChecker fails when sidecar is used without native expression optimizer
            // as the session timezone expected by `now()` will not be passed to the sidecar.
            Session session = Session.builder(getSession())
                    .setSystemProperty(EXPRESSION_OPTIMIZER_NAME, "native")
                    .build();
            MaterializedResult actual = computeActual(session, "VALUES (current_timestamp, now())");

            List<MaterializedRow> rows = actual.getMaterializedRows();
            assertEquals(rows.size(), 1);

            MaterializedRow row = rows.get(0);
            assertEquals(row.getField(0), row.getField(1));
        }
        else {
            super.testValuesWithTimestamp();
        }
    }

    @Override
    @Test
    public void testMinMaxByToWindowFunction()
    {
        if (sidecarEnabled) {
            Session enabled = Session.builder(getSession())
                    .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "true")
                    .setSystemProperty(EXPRESSION_OPTIMIZER_NAME, "native")
                    .build();
            Session disabled = Session.builder(getSession())
                    .setSystemProperty(REWRITE_MIN_MAX_BY_TO_TOP_N, "false")
                    .setSystemProperty(EXPRESSION_OPTIMIZER_NAME, "native")
                    .build();
            @Language("SQL") String sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92])), (1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5])), " +
                    "(7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70])), (2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46])), " +
                    "(5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00])), (4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55])), " +
                    "(8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55])), (6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50])), " +
                    "(2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21])), (1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67])), (7, '2025-01-18', MAP(ARRAY[4, 2, 9], ARRAY[0.80, 0.90, 0.10])), " +
                    "(3, '2025-01-10', MAP(ARRAY[4, 1, 8, 6], ARRAY[0.85, 0.13, 0.42, 0.91])), (8, '2025-01-19', MAP(ARRAY[3, 5], ARRAY[0.15, 0.25])), " +
                    "(4, '2025-01-11', MAP(ARRAY[5, 6], ARRAY[0.11, 0.22])), (5, '2025-01-13', MAP(ARRAY[1, 9], ARRAY[0.66, 0.77])), (6, '2025-01-15', MAP(ARRAY[2, 5], ARRAY[0.10, 0.20])) ) " +
                    "t(id, ds, feature)) select id, max_by(feature, ds), max(ds) from t group by id";

            MaterializedResult result = computeActual(enabled, "explain(type distributed) " + sql);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

            assertQueryWithSameQueryRunner(enabled, sql, disabled);

            sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])), " +
                    "(1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])), (7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])), " +
                    "(2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])), (5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])), " +
                    "(4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])), (8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])), " +
                    "(6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])), (2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])), " +
                    "(1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77])) ) t(id, ds, feature, extra_feature)) " +
                    "select id, max(ds), max_by(feature, ds), max_by(extra_feature, ds) from t group by id";

            result = computeActual(enabled, "explain(type distributed) " + sql);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

            assertQueryWithSameQueryRunner(enabled, sql, disabled);

            sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92])), (1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5])), " +
                    "(7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70])), (2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46])), " +
                    "(5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00])), (4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55])), " +
                    "(8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55])), (6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50])), " +
                    "(2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21])), (1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67])), (7, '2025-01-18', MAP(ARRAY[4, 2, 9], ARRAY[0.80, 0.90, 0.10])), " +
                    "(3, '2025-01-10', MAP(ARRAY[4, 1, 8, 6], ARRAY[0.85, 0.13, 0.42, 0.91])), (8, '2025-01-19', MAP(ARRAY[3, 5], ARRAY[0.15, 0.25])), " +
                    "(4, '2025-01-11', MAP(ARRAY[5, 6], ARRAY[0.11, 0.22])), (5, '2025-01-13', MAP(ARRAY[1, 9], ARRAY[0.66, 0.77])), (6, '2025-01-15', MAP(ARRAY[2, 5], ARRAY[0.10, 0.20])) ) " +
                    "t(id, ds, feature)) select id, min_by(feature, ds), min(ds) from t group by id";

            result = computeActual(enabled, "explain(type distributed) " + sql);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

            assertQueryWithSameQueryRunner(enabled, sql, disabled);

            sql = "with t as (SELECT * FROM ( VALUES (3, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])), " +
                    "(1, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])), (7, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])), " +
                    "(2, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])), (5, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])), " +
                    "(4, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])), (8, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])), " +
                    "(6, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])), (2, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])), " +
                    "(1, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77])) ) t(id, ds, feature, extra_feature)) " +
                    "select id, min(ds), min_by(feature, ds), min_by(extra_feature, ds) from t group by id";

            result = computeActual(enabled, "explain(type distributed) " + sql);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

            assertQueryWithSameQueryRunner(enabled, sql, disabled);

            sql = "with t as (SELECT * FROM ( VALUES (3, 100, '2025-01-08', MAP(ARRAY[2, 1], ARRAY[0.34, 0.92]), MAP(ARRAY['a', 'b'], ARRAY[0.12, 0.88])), " +
                    "(1, 20, '2025-01-02', MAP(ARRAY[1, 3], ARRAY[0.23, 0.5]), MAP(ARRAY['x', 'y'], ARRAY[0.45, 0.55])), (7, 90, '2025-01-17', MAP(ARRAY[6, 8], ARRAY[0.60, 0.70]), MAP(ARRAY['m', 'n'], ARRAY[0.21, 0.79])), " +
                    "(2, 10, '2025-01-06', MAP(ARRAY[2, 3, 5, 7], ARRAY[0.75, 0.32, 0.19, 0.46]), MAP(ARRAY['p', 'q', 'r'], ARRAY[0.11, 0.22, 0.67])), (5, 65, '2025-01-14', MAP(ARRAY[8, 4, 6], ARRAY[0.88, 0.99, 0.00]), MAP(ARRAY['s', 't', 'u'], ARRAY[0.33, 0.44, 0.23])), " +
                    "(4, 40, '2025-01-12', MAP(ARRAY[7, 3, 2], ARRAY[0.33, 0.44, 0.55]), MAP(ARRAY['v', 'w'], ARRAY[0.66, 0.34])), (8, 68, '2025-01-20', MAP(ARRAY[1, 7, 6], ARRAY[0.35, 0.45, 0.55]), MAP(ARRAY['i', 'j', 'k'], ARRAY[0.78, 0.89, 0.12])), " +
                    "(6, 101, '2025-01-16', MAP(ARRAY[9, 1, 3], ARRAY[0.30, 0.40, 0.50]), MAP(ARRAY['c', 'd'], ARRAY[0.90, 0.10])), (2, 35, '2025-01-05', MAP(ARRAY[3, 4], ARRAY[0.98, 0.21]), MAP(ARRAY['e', 'f'], ARRAY[0.56, 0.44])), " +
                    "(1, 25, '2025-01-04', MAP(ARRAY[1, 2], ARRAY[0.45, 0.67]), MAP(ARRAY['g', 'h'], ARRAY[0.23, 0.77])) ) t(id, key, ds, feature, extra_feature)) " +
                    "select id, min(ds), min_by(feature, ds), min_by(extra_feature, ds), min_by(key, ds) from t group by id";

            result = computeActual(enabled, "explain(type distributed) " + sql);
            assertNotEquals(((String) result.getMaterializedRows().get(0).getField(0)).indexOf("TopNRowNumber"), -1);

            assertQueryWithSameQueryRunner(enabled, sql, disabled);
        }
        else {
            super.testMinMaxByToWindowFunction();
        }
    }

    @Override
    @Test
    public void testInUncorrelatedSubquery()
    {
        assertQuery(
                "SELECT CASE WHEN false THEN 1 IN (VALUES 2) END",
                "SELECT NULL");
        if (sidecarEnabled) {
            Session session = Session.builder(getSession())
                    .setSystemProperty(EXPRESSION_OPTIMIZER_NAME, "native")
                    .build();
            assertQuery(session,
                    "SELECT x FROM (VALUES 2) t(x) WHERE MAP(ARRAY[8589934592], ARRAY[x]) IN (VALUES MAP(ARRAY[8589934592],ARRAY[2]))",
                    "SELECT 2");
        }
        else {
            assertQuery("SELECT x FROM (VALUES 2) t(x) WHERE MAP(ARRAY[8589934592], ARRAY[x]) IN (VALUES MAP(ARRAY[8589934592],ARRAY[2]))",
                    "SELECT 2");
        }
        assertQuery(
                "SELECT a IN (VALUES 2), a FROM (VALUES (2)) t(a)",
                "SELECT TRUE, 2");
    }
}
