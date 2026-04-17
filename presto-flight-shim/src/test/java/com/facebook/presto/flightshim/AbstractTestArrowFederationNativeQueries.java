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
package com.facebook.presto.flightshim;

import com.facebook.presto.Session;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.tests.AbstractTestDistributedQueries;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER;
import static com.facebook.presto.SystemSessionProperties.REMOVE_MAP_CAST;
import static com.facebook.presto.tests.QueryAssertions.assertEqualsIgnoreOrder;

@Test(singleThreaded = true)
public abstract class AbstractTestArrowFederationNativeQueries
        extends AbstractTestDistributedQueries
{
    private @Language("RegExp") String unnestRowsInvalidFieldError = "Field not found: field(?:_\\d+)?. Available fields are: field.*";

    @Override
    protected FeaturesConfig createFeaturesConfig()
    {
        return new FeaturesConfig().setNativeExecutionEnabled(true);
    }

    @Override
    protected boolean supportsViews()
    {
        return false;
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

    @Test
    public void testBasic()
    {
        assertQuery("select 1 + 2");
        assertQuery("select * from nation");
        assertQuery("select * from nation where nationkey between 10 and 20");
    }

    @Override
    @Test
    public void testArrayCumSumVarchar()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testApplyLambdaRepeated()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testCustomAdd()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLambdaCapture()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLambdaInAggregationContext()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLambdaInSubqueryContext()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMapBlockBug()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSet()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupBy()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLogGroupByWithNulls()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLogWithNulls()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testNonDeterministicInLambda()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testRowSubscriptInLambda()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testTryWithLambda()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testMergeHyperLogLog()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testCustomSum()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testCustomRank()
    {
        // function not available under native execution
    }

    @Override
    @Test
    public void testLikePrefixAndSuffixWithChars()
    {
        // type not present under native execution
    }

    @Override
    @Test
    public void testMergeKHyperLogLog()
    {
        // type not present under native execution
    }

    @Override
    @Test
    public void testStringFilters()
    {
        // function not available under native execution
    }

    // todo: Add support for INSERT AND CTAS Statements.

    @Override
    @Test
    public void testAddColumn()
    {
    }

    @Override
    @Test
    public void testDropColumn()
    {
    }

    @Override
    @Test
    public void testQueryLoggingCount()
    {
    }

    @Override
    @Test
    public void testRenameColumn()
    {
    }

    @Override
    @Test
    public void testRenameTable()
    {
    }

    @Override
    @Test
    public void testCreateTableAsSelect()
    {
    }

    @Override
    @Test
    public void testInsertIntoNotNullColumn()
    {
    }

    @Override
    @Test
    public void testSymbolAliasing()
    {
    }

    @Override
    @Test
    public void testWrittenStats()
    {
    }

    @Override
    @Test
    public void testCompatibleTypeChangeForView()
    {
    }

    @Override
    @Test
    public void testCompatibleTypeChangeForView2()
    {
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
        @Language("RegExp") String reduceAggInvalidInitialStateError = ".*REDUCE_AGG only supports non-NULL literal as the initial value.*";
        assertQueryFails("select reduce_agg(x, null, (x,y)->try(x+y), (x,y)->try(x+y)) from (select 1 union all select 10) T(x)", reduceAggInvalidInitialStateError);
        assertQueryFails("select reduce_agg(x, cast(null as bigint), (x,y)->coalesce(x, 0)+coalesce(y, 0), (x,y)->coalesce(x, 0)+coalesce(y, 0)) from (values cast(10 as bigint),10)T(x)", reduceAggInvalidInitialStateError);

        // here some reduce_aggs coalesce overflow/zero-divide errors to null in the input/combine functions
        assertQueryFails("select reduce_agg(x, 0, (x,y)->try(1/x+1/y), (x,y)->try(1/x+1/y)) from ((select 0) union all select 10.) T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQueryFails("select reduce_agg(x, 0, (x, y)->try(x+y), (x, y)->try(x+y)) from (values 2817, 9223372036854775807) AS T(x)", "!states->isNullAt\\(i\\) Lambda expressions in reduce_agg should not return null for non-null inputs", true);
        assertQuery("select reduce_agg(x, array[], (x, y)->array[element_at(x, 2)],  (x, y)->array[element_at(x, 2)]) from (select array[array[1]]) T(x)", "select array[null]");
    }

    /// This test is not applicable in Presto C++ since there is no bytecode IR as with JVM.
    @Override
    @Test(enabled = false)
    public void testLargeBytecode() {}

    /// Presto C++ only supports legacy unnest and this test relies on non-legacy behavior of unnest operator for
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

    /// Enabling session property merge_aggregations_with_and_without_filter in Presto C++ clusters without sidecar
    /// results in incorrect results: https://github.com/prestodb/presto/issues/26323.
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

    /// TODO: Check why native query runner doesn't throw an error for certain queries.
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
}
