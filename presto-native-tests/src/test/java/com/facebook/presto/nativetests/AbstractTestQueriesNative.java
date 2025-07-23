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
import com.facebook.presto.tests.AbstractTestQueries;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

public abstract class AbstractTestQueriesNative
        extends AbstractTestQueries
{
    private boolean sidecarEnabled;

    private @Language("RegExp") String arraySplitIntoChunksFunctionUnsupportedError;
    private @Language("RegExp") String p4HllTypeUnsupportedError;
    private @Language("RegExp") String createHllFunctionUnsupportedError;

    public void init(boolean sidecarEnabled)
    {
        this.sidecarEnabled = sidecarEnabled;

        if (sidecarEnabled) {
            arraySplitIntoChunksFunctionUnsupportedError = ".*Function native.default.array_split_into_chunks not registered.*";
            createHllFunctionUnsupportedError = ".*Function native.default.create_hll not registered.*";
            p4HllTypeUnsupportedError = ".*Unknown type: p4hyperloglog.*";
        }
        else {
            arraySplitIntoChunksFunctionUnsupportedError = ".*Scalar function name not registered: presto.default.array_split_into_chunks.*";
            createHllFunctionUnsupportedError = ".*Scalar function name not registered: presto.default.create_hll, called with arguments.*";
            p4HllTypeUnsupportedError = ".*Failed to parse type \\[P4HyperLogLog]. Type not registered.*";
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

    /// array_cum_sum function in Presto C++ does not support decimal array inputs, see issue:
    /// https://github.com/prestodb/presto/issues/24402.
    @Override
    @Test(enabled = false)
    public void testArrayCumSumDecimals() {}

    /// See issue for more details: https://github.com/facebookincubator/velox/issues/10338.
    @Override
    @Test(enabled = false)
    public void testPreserveAssignmentsInJoin() {}

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

    /// TODO: Fix duplicate candidates in function resolution.
    @Override
    @Test(enabled = false)
    public void testApproxPercentile() {}

    /// TODO: Fix result mismatch between Presto and Presto C++.
    /// TODO: Check difference in behavior with sidecar enabled and disabled.
    /// no sidecar
    /// VeloxUserError: numInput == 1 (6000 vs. 1) Expected single row of input. Received 6000 rows.
    /// sidecar
    /// VeloxUserError: numInput == 1 (7500 vs. 1) Expected single row of input. Received 7500 rows.
    @Override
    @Test(enabled = false)
    public void testScalarSubquery() {}

    /// TODO: The decimal coercion queries are flaky in Presto C++, to be investigated further.
    @Override
    @Test(enabled = false)
    public void testCoercions() {}

    /// TODO: Fix result mismatch between Presto and Presto C++.
    /// java.lang.AssertionError: Iterators differ at element [0]: [1, 1, O] != [5, 5, F]
    /// Expected :[1, 1, O]
    /// Actual   :[5, 5, F]
    @Override
    @Test(enabled = false)
    public void testTopNUnpartitionedWindow() {}

    /// TODO: Pending on Presto C++ custom function e2e tests PR: https://github.com/prestodb/presto/pull/25480.
    /// Sidecar is needed to support custom functions in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCustomAdd() {}

    /// TODO: Pending on Velox PR: https://github.com/facebookincubator/velox/pull/14049.
    @Override
    @Test(enabled = false)
    public void testInvalidMapUnionSum() {}

    /// TODO: This test is pending on constant folding support in sidecar.
    /// RemoveMapCastRule requires native constant folding support.
    /// Without sidecar, the integer overflow error message in Presto and Velox differ.
    @Override
    @Test
    public void testRemoveMapCastFailure()
    {
        if (!sidecarEnabled) {
            Session enableOptimization = Session.builder(getSession())
                    .setSystemProperty(REMOVE_MAP_CAST, "true")
                    .build();
            assertQueryFails(enableOptimization, "select feature[key] from (values (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), cast(2 as bigint)), (map(array[cast(1 as integer), 2, 3, 4], array[0.3, 0.5, 0.9, 0.1]), 400000000000)) t(feature, key)",
                    ".*Cannot cast BIGINT '400000000000' to INTEGER. Overflow during arithmetic conversion.*");
        }
    }

    /// TODO: This test is pending on constant folding support in sidecar.
    /// RemoveMapCastRule requires native constant folding support.
    @Override
    @Test
    public void testRemoveMapCast()
    {
        if (!sidecarEnabled) {
            super.testRemoveMapCast();
        }
    }

    /// TODO: Check why this query fails only when sidecar is enabled.
    /// Velox does not support function signature: at_timezone(timestamp with time zone, interval day to second).
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

    /// TODO: key_sampling_percent is a Sql invoked scalar function and these are currently not supported with sidecar.
    /// With the sidecar disabled, this function is evaluated on the coordinator.
    @Override
    @Test
    public void testDefaultSamplingPercent()
    {
        if (sidecarEnabled) {
            @Language("RegExp") String keySamplingPercentFunctionUnsupportedError = ".*Function native.default.key_sampling_percent not registered.*";
            assertQueryFails("select key_sampling_percent('abc')", keySamplingPercentFunctionUnsupportedError);
        }
        else {
            super.testDefaultSamplingPercent();
        }
    }

    /// Varchar(n) is unsupported in Presto C++.
    /// TODO: Check why type is Varchar(N) when sidecar is disabled.
    @Override
    @Test
    public void testDescribeOutput()
    {
        if (sidecarEnabled) {
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
        else {
            super.testDescribeOutput();
        }
    }

    /// Varchar(n) is unsupported in Presto C++.
    /// TODO: Check why type is Varchar(N) when sidecar is disabled.
    @Override
    @Test
    public void testDescribeOutputNamedAndUnnamed()
    {
        if (sidecarEnabled) {
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
        else {
            super.testDescribeOutputNamedAndUnnamed();
        }
    }

    /// Varchar(n) is unsupported in Presto C++.
    /// TODO: Check why this query produces wrong results only when sidecar is enabled.
    @Override
    @Test
    public void testNestedCast()
    {
        if (sidecarEnabled) {
            assertQuery("select cast(varchar_value as varchar(3)) || ' sfd' from (values ('9898.122')) t(varchar_value)", "VALUES '9898.122 sfd'");
            assertQuery("select cast(cast(varchar_value as varchar(3)) as varchar(5)) from (values ('9898.122')) t(varchar_value)", "VALUES '9898.122'");
        }
        else {
            super.testNestedCast();
        }
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

    /// TODO: Check why results do not match for some disabled queries.
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

    /// TODO: Check why results do not match for some queries with MERGE_AGGREGATIONS_WITH_AND_WITHOUT_FILTER enabled.
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

    /// TODO: Check why certain queries are flaky with sidecar disabled.
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

        /// TODO: This query is flaky with sidecar disabled, and occassionally fails with error message:
        /// ".*Function \"ARRAY_SPLIT_INTO_CHUNKS\" not found.*".
        sql = "select array_split_into_chunks(null, 2)";
        if (sidecarEnabled) {
            assertQueryFails(sql, arraySplitIntoChunksFunctionUnsupportedError, true);
        }

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

    /// array_cum_sum does not support Varchar array inputs, the error message differs in Presto C++ and Presto Java.
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

        @Language("RegExp") String hashGenerationUnsupportedError = sidecarEnabled ? ".*Function native.default.\\$operator\\$.*hash.*" : ".*Scalar function name not registered: presto.default.\\$operator\\$.*hash.*";
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

    @Override
    @Test
    public void testFunctionArgumentTypeConstraint()
    {
        @Language("RegExp") String errorMessage = sidecarEnabled ? ".*Function native.default.rgb not registered.*" : ".*Unexpected parameters \\(color\\) for function greatest.*";
        assertQueryFails("SELECT greatest(rgb(255, 0, 0))", errorMessage);
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
    public void testMapBlockBug()
    {
        @Language("RegExp") String mapAggFunctionSignatureUnsupportedError = sidecarEnabled ? ".*Scalar function name not registered: native.default.map_agg.*" : ".*Scalar function name not registered: (presto|native).default.map_agg.*";
        assertQueryFails(" VALUES(MAP_AGG(12345,123))", mapAggFunctionSignatureUnsupportedError, true);
    }

    @Override
    public void testMergeKHyperLogLog()
    {
        @Language("RegExp") String errorMessage = sidecarEnabled ? ".*Function native.default.khyperloglog_agg not registered.*" : ".*Failed to parse type \\[KHyperLogLog]. Type not registered.*";

        assertQueryFails("select k1, cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2) group by k1", errorMessage, true);
        assertQueryFails("select cardinality(merge(khll)), uniqueness_distribution(merge(khll)) from (select k1, k2, khyperloglog_agg(v1, v2) khll from (values (1, 1, 2, 3), (1, 1, 4, 0), (1, 2, 90, 20), (1, 2, 87, 1), " +
                "(2, 1, 11, 30), (2, 1, 11, 11), (2, 2, 9, 1), (2, 2, 87, 2)) t(k1, k2, v1, v2) group by k1, k2)", errorMessage, true);
    }

    /// With sidecar disabled, the expression is constant-folded on the coordinator. Depends on now function in Velox.
    @Override
    @Test
    public void testValuesWithTimestamp()
    {
        if (sidecarEnabled) {
            @Language("RegExp") String nowFunctionUnsupportedError = ".*Function native.default.now not registered.*";
            assertQueryFails("VALUES (current_timestamp, now())", nowFunctionUnsupportedError);
        }
        else {
            super.testValuesWithTimestamp();
        }
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

    /// Inline sql functions should be enabled for key based sampling in Presto C++.
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
}
