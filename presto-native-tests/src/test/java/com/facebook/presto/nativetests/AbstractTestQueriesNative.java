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
import org.intellij.lang.annotations.Language;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.EXPRESSION_OPTIMIZER_NAME;
import static com.facebook.presto.SystemSessionProperties.FIELD_NAMES_IN_JSON_CAST_ENABLED;
import static com.facebook.presto.SystemSessionProperties.REWRITE_MIN_MAX_BY_TO_TOP_N;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

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
    @Test(enabled = false)
    public void testSameAggregationWithAndWithoutFilter() {}

    /// TODO: The last query in this testcase is flaky in Presto C++. See issue
    /// https://github.com/prestodb/presto/issues/27710.
    @Override
    @Test(enabled = false)
    public void testLambdaInAggregation() {}

    /// TODO: Native does not throw the multiple-rows scalar subquery error for certain queries. See issue
    /// https://github.com/prestodb/presto/issues/27709.
    @Override
    @Test(enabled = false)
    public void testCorrelatedNonAggregationScalarSubqueries() {}

    /// Sidecar is needed to support custom functions in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCustomAdd() {}

    /// Sidecar is needed to support custom functions in Presto C++.
    /// TODO: Custom aggregate functions are currently unsupported in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCustomSum() {}

    /// Sidecar is needed to support custom functions in Presto C++.
    /// TODO: Custom window functions are currently unsupported in Presto C++.
    @Override
    @Test(enabled = false)
    public void testCustomRank() {}

    /// TODO: Map union sum should support maps with decimal values, see issue:
    /// https://github.com/prestodb/presto/issues/26659.
    @Override
    @Test(enabled = false)
    public void testInvalidMapUnionSum() {}

    /// This test uses char type which is not supported in Presto C++. See issue:
    /// https://github.com/prestodb/presto/issues/21332.
    @Override
    @Test(enabled = false)
    public void testLikePrefixAndSuffixWithChars() {}

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

    /// The functions are fetched from sidecar when it is enabled so the output of SHOW FUNCTIONS and the order of
    /// functions differ.
    @Override
    @Test(enabled = false)
    public void testShowFunctions() {}

    /// Custom session properties and catalog properties are not supported by native sidecar. Native execution only
    /// system session properties should also be excluded from the result of SHOW SESSION when sidecar is enabled.
    @Override
    @Test(enabled = false)
    public void testShowSession() {}

    /// The integer overflow error message differs in Presto and Velox.
    @Override
    @Test(enabled = false)
    public void testRemoveMapCastFailure() {}

    /// TODO: Velox does not support function signature: at_timezone(timestamp with time zone, interval day to second).
    /// See issue: https://github.com/prestodb/presto/issues/26666.
    @Override
    @Test(enabled = false)
    public void testAtTimeZoneWithInterval() {}

    /// reduce_agg returns different results in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test(enabled = false)
    public void testReduceAgg() {}

    /// reduce_agg returns different results in Presto C++. See Presto C++ limitations here:
    /// https://prestodb.io/docs/current/presto_cpp/limitations.html.
    @Override
    @Test(enabled = false)
    public void testReduceAggWithNulls() {}

    /// array_cum_sum does not support Varchar array inputs, the error message differs in Presto and Velox.
    @Override
    @Test(enabled = false)
    public void testArrayCumSumVarchar() {}

    /// The error message for the case where subquery returns multiple rows differs in Presto and Velox.
    @Override
    @Test(enabled = false)
    public void testScalarSubquery() {}

    /// TODO: Native expression optimizer is required to support system property join_prefilter_build_side in Presto
    /// C++ with sidecar. Pending on https://github.com/prestodb/presto/pull/26475.
    @Override
    @Test(enabled = false)
    public void testJoinPrefilter() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeEmptyNonEmptyApproxSetWithDifferentMaxError() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeHyperLogLog() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeHyperLogLogGroupBy() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeHyperLogLogWithNulls() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeHyperLogLogGroupByWithNulls() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeEmptyNonEmptyApproxSet() {}

    /// create_hll function is not supported in Presto C++, see issue: https://github.com/prestodb/presto/issues/21176.
    @Override
    @Test(enabled = false)
    public void testMergeEmptyNonEmptyApproxSetWithSameMaxError() {}

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
    @Test
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
            // Native expression rewriting is applied during distributed fragment planning.
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
