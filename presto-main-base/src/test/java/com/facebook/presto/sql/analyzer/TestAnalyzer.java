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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.cost.HistoryBasedOptimizationConfig;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.TaskManagerConfig;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.memory.MemoryManagerConfig;
import com.facebook.presto.memory.NodeMemoryConfig;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.StandardWarningCode;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spiller.NodeSpillConfig;
import com.facebook.presto.sql.parser.ParsingException;
import com.facebook.presto.sql.planner.CompilerConfig;
import com.facebook.presto.tracing.TracingConfig;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.regex.Pattern;

import static com.facebook.presto.metadata.SessionPropertyManager.createTestingSessionPropertyManager;
import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.SEMANTIC_WARNING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.AMBIGUOUS_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.CATALOG_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_NAME_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.COLUMN_TYPE_UNKNOWN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_COLUMN_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_PROPERTY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_FUNCTION_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_OFFSET_ROW_COUNT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDER_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_WINDOW_FRAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_COLUMN_ALIASES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISMATCHED_SET_COLUMN_TYPES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ATTRIBUTE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_CATALOG;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_COLUMN;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_ORDER_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_SCHEMA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_AGGREGATION_FUNCTION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MUST_BE_COLUMN_REFERENCE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NESTED_WINDOW;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NON_NUMERIC_SAMPLE_PERCENTAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_AGGREGATE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.ORDER_BY_MUST_BE_IN_SELECT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.PROCEDURE_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SAMPLE_PERCENTAGE_OUT_OF_RANGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.SCHEMA_NOT_SPECIFIED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.STANDALONE_LAMBDA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_COLUMN_NOT_FOUND;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_DUPLICATE_RANGE_VARIABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_COLUMN_REFERENCE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_COPARTITIONING;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_FUNCTION_MISSING_ARGUMENT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TOO_MANY_GROUPING_SETS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_ANALYSIS_ERROR;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_RECURSIVE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.VIEW_IS_STALE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WILDCARD_WITHOUT_FROM;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WINDOW_FUNCTION_ORDERBY_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.WINDOW_REQUIRES_OVER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAnalyzer
        extends AbstractAnalyzerTest
{
    private static void assertHasWarning(WarningCollector warningCollector, StandardWarningCode code, String match)
    {
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertTrue(warnings.size() > 0);
        PrestoWarning warning = warnings.get(0);
        assertEquals(warning.getWarningCode(), code.toWarningCode());
        assertTrue(warning.getMessage().startsWith(match));
    }

    private static void assertNoWarning(WarningCollector warningCollector)
    {
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertTrue(warnings.isEmpty());
    }

    @Test
    public void testNonComparableGroupBy()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1)) GROUP BY 1");
    }

    @Test
    public void testNonComparableWindowPartition()
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (PARTITION BY t.x) FROM (VALUES(CAST (NULL AS HyperLogLog))) AS t(x)");
    }

    @Test
    public void testNonComparableWindowOrder()
    {
        assertFails(TYPE_MISMATCH, "SELECT row_number() OVER (ORDER BY t.x) FROM (VALUES(color('red'))) AS t(x)");
    }

    @Test
    public void testORWarning()
    {
        assertHasWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR t1.b = t2.b"),
                PERFORMANCE_WARNING, "line 1:41: JOIN conditions with an OR can cause performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR t1.a != t2.b AND t1.b > t2.b"),
                PERFORMANCE_WARNING, "line 1:41: JOIN conditions with an OR can cause performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.a != t2.b OR t1.b > t2.b"),
                PERFORMANCE_WARNING, "line 1:58: JOIN conditions with an OR can cause performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a OR IF(t2.b = t1.a OR t2.b = null, 'YES', 'NO') = 'YES'"),
                PERFORMANCE_WARNING, "line 1:41: JOIN conditions with an OR can cause performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON (t1.a = t2.a AND t1.b = t2.b) OR (t1.a > t1.b AND t1.b > t1.a)"),
                PERFORMANCE_WARNING, "line 1:59: JOIN conditions with an OR can cause performance issues as it may lead to a cross join with filter");
    }

    @Test
    void testNoORWarning()
    {
        assertNoWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a"));
        assertNoWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND t1.b = t2.b"));
        assertNoWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a AND IF(t2.b = t1.a OR t2.b = null, 'YES', 'NO') = 'YES'"));
        assertNoWarning(analyzeWithWarnings("SELECT * FROM t1 JOIN t2 ON t1.a = t2.a \n" + "AND (t1.b = t2.b OR t1.b > t2.b)"));
    }

    @Test
    public void testMapFilterWarnings()
    {
        assertNoWarning(analyzeWithWarnings("SELECT map_filter(user_features, (k, v) -> v > 1) FROM (VALUES (map(ARRAY[1,2], ARRAY[2,3]))) AS t(user_features)"));

        assertHasWarning(
                analyzeWithWarnings("SELECT map_filter(user_features, (k, v) -> k = 2) FROM (VALUES (map(ARRAY[1,2,3], ARRAY[10,20,30]))) AS t(user_features)"),
                PERFORMANCE_WARNING,
                "Function 'presto.default.map_filter' uses a lambda on large maps which is expensive. Consider using map_subset");

        assertHasWarning(
                analyzeWithWarnings("SELECT map_filter(user_features, (k, v) -> k IN (1, 3)) FROM (VALUES (map(ARRAY[1,2,3], ARRAY[10,20,30]))) AS t(user_features)"),
                PERFORMANCE_WARNING,
                "Function 'presto.default.map_filter' uses a lambda on large maps which is expensive. Consider using map_subset");

        assertNoWarning(analyzeWithWarnings("SELECT map_filter(user_features, (k, v) -> v IN (20, 30)) FROM (VALUES (map(ARRAY[1,2,3], ARRAY[10,20,30]))) AS t(user_features)"));

        assertNoWarning(analyzeWithWarnings("SELECT map_filter(user_features, (k, v) -> k + v > 25) FROM (VALUES (map(ARRAY[1,2,3], ARRAY[10,20,30]))) AS t(user_features)"));

        assertNoWarning(analyzeWithWarnings("SELECT map_filter(user_features, (k, v) -> k > 2) FROM (VALUES (map(ARRAY[1,2,3], ARRAY[10,20,30]))) AS t(user_features)"));

        assertNoWarning(analyzeWithWarnings("SELECT transform_values(user_features, (k, v) -> v * 2) FROM (VALUES (map(ARRAY[1,2], ARRAY[2,3]))) AS t(user_features)"));

        assertNoWarning(analyzeWithWarnings("SELECT map_filter(x, (k, v) -> k = 2) FROM (VALUES (map(ARRAY[1,2,3], ARRAY[10,20,30]))) AS t(x)"));
    }

    @Test
    public void testIgnoreNullWarning()
    {
        List<String> valueFunctions = ImmutableList.of(
                "NTH_VALUE(c, 1)",
                "FIRST_VALUE(c)",
                "LAST_VALUE(c)",
                "LEAD(c, 1)",
                "LAG(c, 1)");

        for (String function : valueFunctions) {
            assertNoWarning(analyzeWithWarnings("SELECT a, " + function + " IGNORE NULLS OVER\n" +
                    "(ORDER BY b) FROM (VALUES (1, 1, 3), (1, 2, null), (1, 4, 2)) AS t(a, b, c)"));
        }

        List<String> aggAndRankingFunctions = ImmutableList.of(
                "ARRAY_AGG(c)",
                "ARBITRARY(c)",
                "RANK()",
                "DENSE_RANK()");

        for (String function : aggAndRankingFunctions) {
            assertHasWarning(
                    analyzeWithWarnings("SELECT a, " + function + " IGNORE NULLS OVER\n" +
                            "(ORDER BY b) FROM (VALUES (1, 1, 3), (1, 2, null), (1, 4, 2)) AS t(a, b, c)"),
                    SEMANTIC_WARNING,
                    "IGNORE NULLS is not used for aggregate and ranking window functions. This will cause queries to fail in future versions.");
        }
    }

    @Test
    public void testWindowOrderByAnalysis()
    {
        assertHasWarning(analyzeWithWarnings("SELECT SUM(x) OVER (PARTITION BY y ORDER BY 1) AS s\n" +
                "FROM (values (1,10), (2, 10)) AS T(x, y)"), PERFORMANCE_WARNING, "ORDER BY literals/constants with window function:");

        assertHasWarning(analyzeWithWarnings("SELECT SUM(x) OVER (ORDER BY 1) AS s\n" +
                "FROM (values (1,10), (2, 10)) AS T(x, y)"), PERFORMANCE_WARNING, "ORDER BY literals/constants with window function:");

        // Now test for error when the session param is set to disallow this.
        Session session = testSessionBuilder(createTestingSessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig().setAllowWindowOrderByLiterals(false),
                new FunctionsConfig(),
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig(),
                new CompilerConfig(),
                new HistoryBasedOptimizationConfig()))).build();
        assertFails(session, WINDOW_FUNCTION_ORDERBY_LITERAL,
                "SELECT SUM(x) OVER (PARTITION BY y ORDER BY 1) AS s\n" +
                        "FROM (values (1,10), (2, 10)) AS T(x, y)");
        assertFails(session, WINDOW_FUNCTION_ORDERBY_LITERAL,
                "SELECT SUM(x) OVER (ORDER BY 1) AS s\n" +
                        "FROM (values (1,10), (2, 10)) AS T(x, y)");

        analyze(session, "SELECT SUM(x) OVER (PARTITION BY y ORDER BY y) AS s\n" +
                "FROM (values (1,10), (2, 10)) AS T(x, y)");
    }

    @Test
    public void testNonComparableDistinctAggregation()
    {
        assertFails(TYPE_MISMATCH, "SELECT count(DISTINCT x) FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testNonComparableDistinct()
    {
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT * FROM (SELECT approx_set(1) x)");
        assertFails(TYPE_MISMATCH, "SELECT DISTINCT x FROM (SELECT approx_set(1) x)");
    }

    @Test
    public void testInSubqueryTypes()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES 'a') t(y) WHERE y IN (VALUES 1)");
        assertFails(TYPE_MISMATCH, "SELECT (VALUES true) IN (VALUES 1)");
    }

    @Test
    public void testScalarSubQuery()
    {
        analyze("SELECT 'a', (VALUES 1) GROUP BY 1");
        analyze("SELECT 'a', (SELECT (1))");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) = 2");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (VALUES 1)");
        analyze("SELECT * FROM t1 WHERE (VALUES 1) IN (2)");
        analyze("SELECT * FROM (SELECT 1) t1(x) WHERE x IN (SELECT 1)");
    }

    @Test
    public void testReferenceToOutputColumnFromOrderByAggregation()
    {
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT max(a) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY max(a+b)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT DISTINCT a AS a, max(a) AS c from (VALUES (1, 2)) t(a, b) GROUP BY a ORDER BY max(a)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY MAX(a.someField)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(apply(1, z -> x))");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT row_number() over() as a from (values (41, 42), (-41, -42)) t(a,b) group by a+b order by a+b");
    }

    @Test
    public void testHavingReferencesOutputAlias()
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT sum(a) x FROM t1 HAVING x > 5");
    }

    @Test
    public void testWildcardWithInvalidPrefix()
    {
        assertFails(MISSING_TABLE, "SELECT foo.* FROM t1");
    }

    @Test
    public void testGroupByWithWildcard()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT * FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT u1.*, u2.* FROM (select a, b + 1 from t1) u1 JOIN (select a, b + 2 from t1) u2 ON u1.a = u2.a GROUP BY u1.a, u2.a, 3");
    }

    @Test
    public void testGroupByInvalidOrdinal()
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 GROUP BY 0");
    }

    @Test
    public void testGroupByWithSubquerySelectExpression()
    {
        analyze("SELECT (SELECT t1.a) FROM t1 GROUP BY a");
        analyze("SELECT (SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyze("SELECT (SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:16: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT (SELECT u.a from (values 1) x(a)) FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:16: Subquery uses 'a' which must appear in GROUP BY clause",
                "SELECT (SELECT a+2) FROM t1 GROUP BY a+1");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:36: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT (SELECT 1 FROM t1 WHERE a = u.a) FROM t1 u GROUP BY b");

        // (t1.)a is not part of GROUP BY
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT (SELECT a as a) FROM t1 GROUP BY b");

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyze("SELECT (SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b");
    }

    @Test
    public void testGroupByWithExistsSelectExpression()
    {
        analyze("SELECT EXISTS(SELECT t1.a) FROM t1 GROUP BY a");
        analyze("SELECT EXISTS(SELECT a) FROM t1 GROUP BY t1.a");

        // u.a is not GROUP-ed BY and it is used in select Subquery expression
        analyze("SELECT EXISTS(SELECT u.a FROM (values 1) u(a)) " +
                "FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:22: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT EXISTS(SELECT u.a from (values 1) x(a)) FROM t1 u GROUP BY b");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:22: Subquery uses 'a' which must appear in GROUP BY clause",
                "SELECT EXISTS(SELECT a+2) FROM t1 GROUP BY a+1");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:42: Subquery uses 'u.a' which must appear in GROUP BY clause",
                "SELECT EXISTS(SELECT 1 FROM t1 WHERE a = u.a) FROM t1 u GROUP BY b");

        // (t1.)a is not part of GROUP BY
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT EXISTS(SELECT a as a) FROM t1 GROUP BY b");

        // u.a is not GROUP-ed BY but select Subquery expression is using a different (shadowing) u.a
        analyze("SELECT EXISTS(SELECT 1 FROM t1 u WHERE a = u.a) FROM t1 u GROUP BY b");
    }

    @Test
    public void testGroupByWithSubquerySelectExpressionWithDereferenceExpression()
    {
        analyze("SELECT (SELECT t.a.someField) " +
                "FROM (VALUES ROW(CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(a, b) " +
                "GROUP BY a");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:16: Subquery uses 't.a' which must appear in GROUP BY clause",
                "SELECT (SELECT t.a.someField) " +
                        "FROM (VALUES ROW(CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(a, b) " +
                        "GROUP BY b");
    }

    @Test
    public void testOrderByInvalidOrdinal()
    {
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 10");
        assertFails(INVALID_ORDINAL, "SELECT * FROM t1 ORDER BY 0");
    }

    @Test
    public void testOrderByNonComparable()
    {
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (SELECT approx_set(1) x) ORDER BY 1");
        assertFails(TYPE_MISMATCH, "SELECT x FROM (SELECT approx_set(1) x) ORDER BY x");
    }

    @Test
    public void testOffsetInvalidRowCount()
    {
        assertFails(INVALID_OFFSET_ROW_COUNT, "SELECT * FROM t1 OFFSET 987654321098765432109876543210 ROWS");
    }

    @Test
    public void testNestedAggregation()
    {
        assertFails(NESTED_AGGREGATION, "SELECT sum(count(*)) FROM t1");
    }

    @Test
    public void testAggregationsNotAllowed()
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 WHERE sum(a) > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 GROUP BY sum(a)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) = t2.a");
    }

    @Test
    public void testWindowsNotAllowed()
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 WHERE pi() over () > 1");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 GROUP BY rank() over ()");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT * FROM t1 JOIN t2 ON sum(t1.a) over () = t2.a");
        assertFails(NESTED_WINDOW, "SELECT 1 FROM (VALUES 1) HAVING count(*) OVER () > 1");
    }

    @Test
    public void testCallProcedure()
    {
        Session session = testSessionBuilder()
                .setCatalog("c2")
                .setSchema("t4")
                .build();
        assertFails(session, PROCEDURE_NOT_FOUND, "call system.not_exist_procedure('a', 'b')");
        assertFails(session, PROCEDURE_NOT_FOUND, "call system.procedure('a', 'b')");
        assertFails(session, MISSING_SCHEMA, "call system.distributed_procedure('s1', 't4')");
        assertFails(session, MISSING_TABLE, "call system.distributed_procedure('s2', 't9')");
        analyze(session, "call system.distributed_procedure('s2', 't4')");
    }

    @Test
    public void testGrouping()
    {
        analyze("SELECT a, b, sum(c), grouping(a, b) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))");
        analyze("SELECT grouping(t1.a) FROM t1 GROUP BY a");
        analyze("SELECT grouping(b) FROM t1 GROUP BY t1.b");
        analyze("SELECT grouping(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupingNotAllowed()
    {
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT a, b, sum(c) FROM t1 WHERE grouping(a, b) GROUP BY GROUPING SETS ((a), (a, b))");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT a, b, sum(c) FROM t1 GROUP BY grouping(a, b)");
        assertFails(CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING, "SELECT t1.a, t1.b FROM t1 JOIN t2 ON grouping(t1.a, t1.b) > t2.a");

        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT grouping(a) FROM t1");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT * FROM t1 ORDER BY grouping(a)");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT grouping(a) FROM t1 GROUP BY b");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT grouping(a.field) FROM (VALUES ROW(CAST(ROW(1) AS ROW(field BIGINT)))) t(a) GROUP BY a.field");

        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING, "SELECT a FROM t1 GROUP BY a ORDER BY grouping(a)");
    }

    @Test
    public void testGroupingTooManyArguments()
    {
        String grouping = "GROUPING(a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a, a," +
                "a, a)";
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, %s + 1 FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping));
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, %s as g FROM t1 GROUP BY a, b HAVING g > 0", grouping));
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, rank() OVER (PARTITION BY %s) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping));
        assertFails(INVALID_PROCEDURE_ARGUMENTS, String.format("SELECT a, b, rank() OVER (PARTITION BY a ORDER BY %s) FROM t1 GROUP BY GROUPING SETS ((a), (a, b))", grouping));
    }

    @Test
    public void testInvalidTable()
    {
        assertFails(MISSING_CATALOG, "SELECT * FROM foo.bar.t");
        assertFails(MISSING_SCHEMA, "SELECT * FROM foo.t");
        assertFails(MISSING_TABLE, "SELECT * FROM foo");
    }

    @Test
    public void testInvalidSchema()
    {
        assertFails(MISSING_SCHEMA, "SHOW TABLES FROM NONEXISTENT_SCHEMA");
        assertFails(MISSING_SCHEMA, "SHOW TABLES IN NONEXISTENT_SCHEMA LIKE '%'");
    }

    @Test
    public void testNonAggregate()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 'a', array[b][1] FROM t1 GROUP BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) / a FROM t1 GROUP BY c");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT sum(b) FROM t1 ORDER BY a + 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a, sum(b) FROM t1 GROUP BY a HAVING c > 5");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (PARTITION BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY a) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN b PRECEDING AND a PRECEDING) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT count(*) over (ORDER BY count(*) ROWS BETWEEN a PRECEDING AND UNBOUNDED PRECEDING) FROM t1 GROUP BY b");
    }

    @Test
    public void testInvalidAttribute()
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT f FROM t1");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 ORDER BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT count(*) FROM t1 GROUP BY f");
        assertFails(MISSING_ATTRIBUTE, "SELECT * FROM t1 WHERE f > 1");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = "line 1:8: 't.y' cannot be resolved")
    public void testInvalidAttributeCorrectErrorMessage()
    {
        analyze("SELECT t.y FROM (VALUES 1) t(x)");
    }

    @Test
    public void testOrderByMustAppearInSelectWithDistinct()
    {
        assertFails(ORDER_BY_MUST_BE_IN_SELECT, "SELECT DISTINCT a FROM t1 ORDER BY b");
    }

    @Test
    public void testNonDeterministicOrderBy()
    {
        analyze("SELECT DISTINCT random() as b FROM t1 ORDER BY b");
        analyze("SELECT random() FROM t1 ORDER BY random()");
        analyze("SELECT a FROM t1 ORDER BY random()");
        assertFails(NONDETERMINISTIC_ORDER_BY_EXPRESSION_WITH_SELECT_DISTINCT, "SELECT DISTINCT random() FROM t1 ORDER BY random()");
    }

    @Test
    public void testNonBooleanWhereClause()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE a");
    }

    @Test
    public void testDistinctAggregations()
    {
        analyze("SELECT COUNT(DISTINCT a), SUM(a) FROM t1");
    }

    @Test
    public void testMultipleDistinctAggregations()
    {
        analyze("SELECT COUNT(DISTINCT a), COUNT(DISTINCT b) FROM t1");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn()
    {
        // TODO: analyze output
        analyze("SELECT a x FROM t1 ORDER BY x + 1");
        analyze("SELECT max(a) FROM (values (1,2), (2,1)) t(a,b) GROUP BY b ORDER BY max(b*1e0)");
        analyze("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS a FROM (values (1,2)) t(a,b) GROUP BY b ORDER BY a.someField");
        analyze("SELECT 1 AS x FROM (values (1,2)) t(x, y) GROUP BY y ORDER BY sum(apply(1, x -> x))");
    }

    @Test
    public void testOrderByExpressionOnOutputColumn2()
    {
        // TODO: validate output
        analyze("SELECT a x FROM t1 ORDER BY a + 1");

        assertFails(TYPE_MISMATCH, 3, 10,
                "SELECT x.c as x\n" +
                        "FROM (VALUES 1) x(c)\n" +
                        "ORDER BY x.c");
    }

    @Test
    public void testOrderByWithWildcard()
    {
        // TODO: validate output
        analyze("SELECT t1.* FROM t1 ORDER BY a");
    }

    @Test
    public void testOrderByWithGroupByAndSubquerySelectExpression()
    {
        analyze("SELECT a FROM t1 GROUP BY a ORDER BY (SELECT a)");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:46: Subquery uses 'b' which must appear in GROUP BY clause",
                "SELECT a FROM t1 GROUP BY a ORDER BY (SELECT b)");

        analyze("SELECT a AS b FROM t1 GROUP BY t1.a ORDER BY (SELECT b)");

        assertFails(
                REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                "line 2:22: Invalid reference to output projection attribute from ORDER BY aggregation",
                "SELECT a AS b FROM t1 GROUP BY t1.a \n" +
                        "ORDER BY MAX((SELECT b))");

        analyze("SELECT a FROM t1 GROUP BY a ORDER BY MAX((SELECT x FROM (VALUES 4) t(x)))");

        analyze("SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                "FROM (VALUES (1, 2)) t(a, b)\n" +
                "GROUP BY b\n" +
                "ORDER BY (SELECT x.someField)");

        assertFails(
                REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                "line 4:22: Invalid reference to output projection attribute from ORDER BY aggregation",
                "SELECT CAST(ROW(1) AS ROW(someField BIGINT)) AS x\n" +
                        "FROM (VALUES (1, 2)) t(a, b)\n" +
                        "GROUP BY b\n" +
                        "ORDER BY MAX((SELECT x.someField))");
    }

    @Test
    public void testTooManyGroupingElements()
    {
        Session session = testSessionBuilder(createTestingSessionPropertyManager(new SystemSessionProperties(
                new QueryManagerConfig(),
                new TaskManagerConfig(),
                new MemoryManagerConfig(),
                new FeaturesConfig().setMaxGroupingSets(2048),
                new FunctionsConfig(),
                new NodeMemoryConfig(),
                new WarningCollectorConfig(),
                new NodeSchedulerConfig(),
                new NodeSpillConfig(),
                new TracingConfig(),
                new CompilerConfig(),
                new HistoryBasedOptimizationConfig()))).build();
        analyze(session, "SELECT a, b, c, d, e, f, g, h, i, j, k, SUM(l)" +
                "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12))\n" +
                "t (a, b, c, d, e, f, g, h, i, j, k, l)\n" +
                "GROUP BY CUBE (a, b, c, d, e, f), CUBE (g, h, i, j, k)");
        assertFails(session, TOO_MANY_GROUPING_SETS,
                "line 3:10: GROUP BY has 4096 grouping sets but can contain at most 2048",
                "SELECT a, b, c, d, e, f, g, h, i, j, k, l, SUM(m)" +
                        "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13))\n" +
                        "t (a, b, c, d, e, f, g, h, i, j, k, l, m)\n" +
                        "GROUP BY CUBE (a, b, c, d, e, f), CUBE (g, h, i, j, k, l)");
        assertFails(session, TOO_MANY_GROUPING_SETS,
                format("line 3:10: GROUP BY has more than %s grouping sets but can contain at most 2048", Integer.MAX_VALUE),
                "SELECT a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                        "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae, SUM(af)" +
                        "FROM (VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, " +
                        "17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32))\n" +
                        "t (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                        "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae, af)\n" +
                        "GROUP BY CUBE (a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, p, " +
                        "q, r, s, t, u, v, x, w, y, z, aa, ab, ac, ad, ae)");
    }

    @Test
    public void testMismatchedColumnAliasCount()
    {
        assertFails(MISMATCHED_COLUMN_ALIASES, "SELECT * FROM t1 u (x, y)");
    }

    @Test
    public void testJoinOnConstantExpression()
    {
        analyze("SELECT * FROM t1 JOIN t2 ON 1 = 1");
    }

    @Test
    public void testJoinOnNonBooleanExpression()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 JOIN t2 ON 5");
    }

    @Test
    public void testJoinOnAmbiguousName()
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT * FROM t1 JOIN t2 ON a = a");
    }

    @Test
    public void testNonEquiOuterJoin()
    {
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 RIGHT JOIN t2 ON t1.a + t2.a = 1");
        analyze("SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.a OR t1.b = t2.b");
    }

    @Test
    public void testNonBooleanHaving()
    {
        assertFails(TYPE_MISMATCH, "SELECT sum(a) FROM t1 HAVING sum(a)");
    }

    @Test
    public void testAmbiguousReferenceInOrderBy()
    {
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, b x FROM t1 ORDER BY x");
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a x, a x FROM t1 ORDER BY x");
        assertFails(AMBIGUOUS_ATTRIBUTE, "SELECT a, a FROM t1 ORDER BY a");
    }

    @Test
    public void testImplicitCrossJoin()
    {
        // TODO: validate output
        analyze("SELECT * FROM t1, t2");
    }

    @Test
    public void testNaturalJoinNotSupported()
    {
        assertFails(NOT_SUPPORTED, "SELECT * FROM t1 NATURAL JOIN t2");
    }

    @Test
    public void testNestedWindowFunctions()
    {
        assertFails(NESTED_WINDOW, "SELECT avg(sum(a) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT sum(sum(a) OVER ()) OVER () FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (PARTITION BY sum(b) OVER ()) FROM t1");
        assertFails(NESTED_WINDOW, "SELECT avg(a) OVER (ORDER BY sum(b) OVER ()) FROM t1");
    }

    @Test
    public void testWindowFunctionWithoutOverClause()
    {
        assertFails(WINDOW_REQUIRES_OVER, "SELECT row_number()");
        assertFails(WINDOW_REQUIRES_OVER, "SELECT coalesce(lead(a), 0) from (values(0)) t(a)");
    }

    @Test
    public void testInvalidWindowFrameTypeRows()
    {
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS UNBOUNDED FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS 2 FOLLOWING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND 5 PRECEDING)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ROWS BETWEEN 2 FOLLOWING AND CURRENT ROW)");

        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 5e-1 PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS 'foo' PRECEDING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 5e-1 FOLLOWING)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ROWS BETWEEN CURRENT ROW AND 'foo' FOLLOWING)");
    }

    @Test
    public void testWindowFrameTypeRange()
    {
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND 2 FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND 5 PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED FOLLOWING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE 2 FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND CURRENT ROW) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 5 PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 5 PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND UNBOUNDED PRECEDING) FROM (VALUES 1) T(x)");

        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE UNBOUNDED PRECEDING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND 5 PRECEDING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND 2 FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE 5 PRECEDING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND 10 PRECEDING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND 3 PRECEDING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND CURRENT ROW) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND 2 FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 5 PRECEDING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE CURRENT ROW) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND 2 FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 1 FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND 10 FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 2 FOLLOWING AND UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");

        // this should pass the analysis but fail during execution
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN -x PRECEDING AND 0 * x FOLLOWING) FROM (VALUES 1) T(x)");
        analyze("SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN CAST(null AS BIGINT) PRECEDING AND CAST(null AS BIGINT) FOLLOWING) FROM (VALUES 1) T(x)");

        assertFails(MISSING_ORDER_BY, "SELECT array_agg(x) OVER (RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 1) T(x)");

        assertFails(INVALID_ORDER_BY, "SELECT array_agg(x) OVER (ORDER BY x DESC, x ASC RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 1) T(x)");

        assertFails(TYPE_MISMATCH, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM (VALUES 'a') T(x)");

        assertFails(TYPE_MISMATCH, "SELECT array_agg(x) OVER (ORDER BY x RANGE BETWEEN 'a' PRECEDING AND 'z' FOLLOWING) FROM (VALUES 1) T(x)");

        assertFails(TYPE_MISMATCH, "SELECT array_agg(x) OVER (ORDER BY x RANGE INTERVAL '1' day PRECEDING) FROM (VALUES INTERVAL '1' year) T(x)");

        // window frame other than <expression> PRECEDING or <expression> FOLLOWING has no requirements regarding window ORDER BY clause
        // ORDER BY is not required
        analyze("SELECT array_agg(x) OVER (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        // multiple sort keys and sort keys of types other than numeric or datetime are allowed
        analyze("SELECT array_agg(x) OVER (ORDER BY y, z RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) FROM (VALUES (1, 'text', true)) T(x, y, z)");
    }

    @Test
    public void testInvalidWindowFrameTypeGroups()
    {
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS UNBOUNDED FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS 2 FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN UNBOUNDED FOLLOWING AND CURRENT ROW) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND UNBOUNDED PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND 5 PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN 2 FOLLOWING AND 5 PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(INVALID_WINDOW_FRAME, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN 2 FOLLOWING AND CURRENT ROW) FROM (VALUES 1) T(x)");

        assertFails(MISSING_ORDER_BY, "SELECT rank() OVER (GROUPS 2 PRECEDING) FROM (VALUES 1) T(x)");

        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ORDER BY x GROUPS 5e-1 PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ORDER BY x GROUPS 'foo' PRECEDING) FROM (VALUES 1) T(x)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND 5e-1 FOLLOWING) FROM (VALUES 1) T(x)");
        assertFails(TYPE_MISMATCH, "SELECT rank() OVER (ORDER BY x GROUPS BETWEEN CURRENT ROW AND 'foo' FOLLOWING) FROM (VALUES 1) T(x)");
    }

    @Test
    public void testDistinctInWindowFunctionParameter()
    {
        assertFails(NOT_SUPPORTED, "SELECT a, count(DISTINCT b) OVER () FROM t1");
    }

    @Test
    public void testGroupByOrdinalsWithWildcard()
    {
        // TODO: verify output
        analyze("SELECT t1.*, a FROM t1 GROUP BY 1,2,c,d");
    }

    @Test
    public void testGroupByWithQualifiedName()
    {
        // TODO: verify output
        analyze("SELECT a FROM t1 GROUP BY t1.a");
    }

    @Test
    public void testGroupByWithQualifiedName2()
    {
        // TODO: verify output
        analyze("SELECT t1.a FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByWithQualifiedName3()
    {
        // TODO: verify output
        analyze("SELECT * FROM t1 GROUP BY t1.a, t1.b, t1.c, t1.d");
    }

    @Test
    public void testGroupByWithRowExpression()
    {
        // TODO: verify output
        analyze("SELECT (a, b) FROM t1 GROUP BY a, b");
    }

    @Test
    public void testHaving()
    {
        // TODO: verify output
        analyze("SELECT sum(a) FROM t1 HAVING avg(a) - avg(b) > 10");
    }

    @Test
    public void testWithCaseInsensitiveResolution()
    {
        // TODO: verify output
        analyze("WITH AB AS (SELECT * FROM t1) SELECT * FROM ab");
    }

    @Test
    public void testStartTransaction()
    {
        analyze("START TRANSACTION");
        analyze("START TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        analyze("START TRANSACTION ISOLATION LEVEL READ COMMITTED");
        analyze("START TRANSACTION ISOLATION LEVEL REPEATABLE READ");
        analyze("START TRANSACTION ISOLATION LEVEL SERIALIZABLE");
        analyze("START TRANSACTION READ ONLY");
        analyze("START TRANSACTION READ WRITE");
        analyze("START TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY");
        analyze("START TRANSACTION READ ONLY, ISOLATION LEVEL READ COMMITTED");
        analyze("START TRANSACTION READ WRITE, ISOLATION LEVEL SERIALIZABLE");
    }

    @Test
    public void testCommit()
    {
        analyze("COMMIT");
        analyze("COMMIT WORK");
    }

    @Test
    public void testRollback()
    {
        analyze("ROLLBACK");
        analyze("ROLLBACK WORK");
    }

    @Test
    public void testExplainAnalyze()
    {
        analyze("EXPLAIN ANALYZE SELECT * FROM t1");
    }

    @Test
    public void testExplainAnalyzeFormatJson()
    {
        analyze("EXPLAIN ANALYZE (format JSON) SELECT * FROM t1");
    }

    public void testExplainAnalyzeFormatJsonHasStats()
    {
        analyze("EXPLAIN ANALYZE (format JSON) SELECT * FROM t1");
    }

    @Test
    public void testExplainAnalyzeFormatJsonTypeDistributed()
    {
        analyze("EXPLAIN ANALYZE (format JSON, type DISTRIBUTED) SELECT * FROM t1");
    }

    @Test
    public void testExplainAnalyzeIllegalArgs()
    {
        assertThrows(IllegalStateException.class, () -> analyze("EXPLAIN ANALYZE (type LOGICAL) SELECT * FROM t1"));
        assertThrows(IllegalStateException.class, () -> analyze("EXPLAIN ANALYZE (format TEXT, type LOGICAL) SELECT * FROM t1"));
        assertThrows(IllegalStateException.class, () -> analyze("EXPLAIN ANALYZE (format JSON, format TEXT) SELECT * FROM t1"));
    }

    @Test
    public void testInsert()
    {
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "Mismatch at column 1: 'a' is of type bigint but expression is of type varchar", "INSERT INTO t6 (a) SELECT b from t6");
        analyze("INSERT INTO t1 SELECT * FROM t1");
        analyze("INSERT INTO t3 SELECT * FROM t3");
        analyze("INSERT INTO t3 SELECT a, b FROM t3");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 VALUES (1, 2)");
        analyze("INSERT INTO t5 (a) VALUES(null)");

        // ignore t5 hidden column
        analyze("INSERT INTO t5 VALUES (1)");

        // fail if hidden column provided
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t5 VALUES (1, 2)");

        // note b is VARCHAR, while a,c,d are BIGINT
        analyze("INSERT INTO t6 (a) SELECT a from t6");
        analyze("INSERT INTO t6 (a) SELECT c from t6");
        analyze("INSERT INTO t6 (a,b,c,d) SELECT * from t6");
        analyze("INSERT INTO t6 (A,B,C,D) SELECT * from t6");
        analyze("INSERT INTO t6 (a,b,c,d) SELECT d,b,c,a from t6");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t6 (a) SELECT b from t6");
        assertFails(MISSING_COLUMN, "INSERT INTO t6 (unknown) SELECT * FROM t6");
        assertFails(DUPLICATE_COLUMN_NAME, "INSERT INTO t6 (a, a) SELECT * FROM t6");
        assertFails(DUPLICATE_COLUMN_NAME, "INSERT INTO t6 (a, A) SELECT * FROM t6");

        // b is bigint, while a is double, coercion from b to a is possible
        analyze("INSERT INTO t7 (b) SELECT (a) FROM t7 ");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t7 (a) SELECT (b) FROM t7");

        // d is array of bigints, while c is array of doubles, coercion from d to c is possible
        analyze("INSERT INTO t7 (d) SELECT (c) FROM t7 ");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t7 (c) SELECT (d) FROM t7 ");

        analyze("INSERT INTO t7 (d) VALUES (ARRAY[null])");

        analyze("INSERT INTO t6 (d) VALUES (1), (2), (3)");
        analyze("INSERT INTO t6 (a,b,c,d) VALUES (1, 'a', 1, 1), (2, 'b', 2, 2), (3, 'c', 3, 3), (4, 'd', 4, 4)");
    }

    @Test
    public void testInvalidInsert()
    {
        assertFails(MISSING_TABLE, "INSERT INTO foo VALUES (1)");
        assertFails(NOT_SUPPORTED, "INSERT INTO v1 VALUES (1)");

        // fail if inconsistent fields count
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a) VALUES (1), (1, 2)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES (1), (1, 2)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES (1, 2), (1, 2), (1, 2, 3)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES ('a', 'b'), ('a', 'b', 'c')");
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:51: Insert query has 5 expression.s. but expected 4 target column.s.. Mismatch at column 5",
                "INSERT INTO t6 (a, b, c, d) VALUES (1, 'a', 1, 1, 1)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:38: Insert query has 5 expression.s. but expected 4 target column.s.. Mismatch at column 5",
                "INSERT INTO t6 VALUES (1, 'a', 1, 1, 1)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 3:3: Insert query has 3 expression.s. but expected 4 target column.s.. Mismatch at column 4: 'd'",
                "INSERT INTO t6 (a, b, c, d) VALUES (1\n, 'a'\n, 1)");

        // fail if mismatched column types
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:45: Mismatch at column 3: 'c' is of type bigint but expression is of type varchar.*",
                "INSERT INTO t6 (a, b, c, d) VALUES (1, 'a', 'a', 1)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES ('a', 'b'), (1, 'b')");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "INSERT INTO t1 (a, b) VALUES ('a', 'b'), ('a', 'b'), (1, 'b')");

        // show size mismatch as well as the first mismatched column position
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:45: Insert query has 5 expression.s. but expected 4 target column.s.. Mismatch at column 3: 'c' is of type bigint but expression is of type varchar.*",
                "INSERT INTO t6 (a, b, c, d) VALUES (1, 'a', 'a', 1, 1)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:32: Insert query has 5 expression.s. but expected 4 target column.s.. Mismatch at column 3: 'c' is of type bigint but expression is of type varchar.*",
                "INSERT INTO t6 VALUES (1, 'a', 'a', 1, 1)");
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:40: Insert query has 3 expression.s. but expected 4 target column.s.. Mismatch at column 2: 'b' is of type varchar but expression is of type integer",
                "INSERT INTO t6 (a, b, c, d) VALUES (1, 1, 1)");
    }

    @Test
    public void testInvalidInsertNested()
    {
        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:29: Mismatch at column 2: 'b.x.z' is of type double but expression is of type varchar.3.",
                "INSERT INTO t10 VALUES (10, ROW(20, ROW(30, 'abc')), ROW(40))");

        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:29: Mismatch at column 2: 'b.x' has 2 field.s. but expression has 3 field.s.",
                "INSERT INTO t10 VALUES (10, ROW(20, ROW(30, 3, 10)), ROW(40))");

        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:29: Mismatch at column 2: 'b.w' is of type bigint but expression is of type varchar.3.",
                "INSERT INTO t10 VALUES (10, ROW('abc', ROW(30, 40)), ROW(40))");

        assertFails(MISMATCHED_SET_COLUMN_TYPES,
                "line 1:51: Mismatch at column 3: 'c.d' is of type bigint but expression is of type varchar.3.",
                "INSERT INTO t10 VALUES (10, ROW(20, ROW(30, 40)), ROW('abc'))");
    }

    @Test
    public void testDuplicateWithQuery()
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     a AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testCaseInsensitiveDuplicateWithQuery()
    {
        assertFails(DUPLICATE_RELATION,
                "WITH a AS (SELECT * FROM t1)," +
                        "     A AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testWithForwardReference()
    {
        assertFails(MISSING_TABLE,
                "WITH a AS (SELECT * FROM b)," +
                        "     b AS (SELECT * FROM t1)" +
                        "SELECT * FROM a");
    }

    @Test
    public void testExpressions()
    {
        // logical not
        assertFails(TYPE_MISMATCH, "SELECT NOT 1 FROM t1");

        // logical and/or
        assertFails(TYPE_MISMATCH, "SELECT 1 AND TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE AND 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 OR TRUE FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT TRUE OR 1 FROM t1");

        // comparison
        assertFails(TYPE_MISMATCH, "SELECT 1 = 'a' FROM t1");

        // nullif
        assertFails(TYPE_MISMATCH, "SELECT NULLIF(1, 'a') FROM t1");

        // case
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN TRUE THEN 'a' ELSE 1 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE WHEN '1' THEN 1 ELSE 2 END FROM t1");

        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 'a' THEN 2 END FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT CASE 1 WHEN 1 THEN 2 ELSE 'a' END FROM t1");

        // coalesce
        assertFails(TYPE_MISMATCH, "SELECT COALESCE(1, 'a') FROM t1");

        // cast
        assertFails(TYPE_MISMATCH, "SELECT CAST(date '2014-01-01' AS bigint)");
        assertFails(TYPE_MISMATCH, "SELECT TRY_CAST(date '2014-01-01' AS bigint)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(null AS UNKNOWN)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(1 AS MAP)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(1 AS ARRAY)");
        assertFails(TYPE_MISMATCH, "SELECT CAST(1 AS ROW)");

        // arithmetic unary
        assertFails(TYPE_MISMATCH, "SELECT -'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT +'a' FROM t1");

        // arithmetic addition/subtraction
        assertFails(TYPE_MISMATCH, "SELECT 'a' + 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 + 'a'  FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' - 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 - 'a' FROM t1");

        // like
        assertFails(TYPE_MISMATCH, "SELECT 1 LIKE 'a' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 1 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 'a' LIKE 'b' ESCAPE 1 FROM t1");

        // extract
        assertFails(TYPE_MISMATCH, "SELECT EXTRACT(DAY FROM 'a') FROM t1");

        // between
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 2 FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 0 AND 'b' FROM t1");
        assertFails(TYPE_MISMATCH, "SELECT 1 BETWEEN 'a' AND 'b' FROM t1");

        // in
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 1 IN ('a')");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE 'a' IN (1, 'b')");

        // row type
        assertFails(TYPE_MISMATCH, "SELECT t.x.f1 FROM (VALUES 1) t(x)");
        assertFails(TYPE_MISMATCH, "SELECT x.f1 FROM (VALUES 1) t(x)");

        // subscript on Row
        assertFails(INVALID_PARAMETER_USAGE, "line 1:20: Subscript expression on ROW requires a constant index", "SELECT ROW(1, 'a')[x]");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:20: Subscript expression on ROW requires a constant index", "SELECT ROW(1, 'a')[-1]");
        assertFails(TYPE_MISMATCH, "line 1:20: Subscript expression on ROW requires integer index, found bigint", "SELECT ROW(1, 'a')[9999999999]");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:20: Invalid subscript index: 0. ROW indices start at 1", "SELECT ROW(1, 'a')[0]");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:20: Subscript index out of bounds: 5, max value is 2", "SELECT ROW(1, 'a')[5]");
    }

    @Test
    public void testLike()
    {
        analyze("SELECT '1' LIKE '1'");
        analyze("SELECT CAST('1' as CHAR(1)) LIKE '1'");
    }

    @Test(enabled = false) // TODO: need to support widening conversion for numbers
    public void testInWithNumericTypes()
    {
        analyze("SELECT * FROM t1 WHERE 1 IN (1, 2, 3.5)");
    }

    @Test
    public void testWildcardWithoutFrom()
    {
        assertFails(WILDCARD_WITHOUT_FROM, "SELECT *");
    }

    @Test
    public void testReferenceWithoutFrom()
    {
        assertFails(MISSING_ATTRIBUTE, "SELECT dummy");
    }

    @Test
    public void testGroupBy()
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a");
    }

    @Test
    public void testGroupByEmpty()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a FROM t1 GROUP BY ()");
    }

    @Test
    public void testComplexExpressionInGroupingSet()
    {
        assertFails(
                MUST_BE_COLUMN_REFERENCE,
                "\\Qline 1:49: GROUP BY expression must be a column reference: (x + 1)\\E",
                "SELECT 1 FROM (VALUES 1) t(x) GROUP BY ROLLUP(x + 1)");
        assertFails(
                MUST_BE_COLUMN_REFERENCE,
                "\\Qline 1:47: GROUP BY expression must be a column reference: (x + 1)\\E",
                "SELECT 1 FROM (VALUES 1) t(x) GROUP BY CUBE(x + 1)");
        assertFails(
                MUST_BE_COLUMN_REFERENCE,
                "\\Qline 1:57: GROUP BY expression must be a column reference: (x + 1)\\E",
                "SELECT 1 FROM (VALUES 1) t(x) GROUP BY GROUPING SETS (x + 1)");

        assertFails(
                MUST_BE_COLUMN_REFERENCE,
                "\\Qline 1:52: GROUP BY expression must be a column reference: (x + 1)\\E",
                "SELECT 1 FROM (VALUES 1) t(x) GROUP BY ROLLUP(x, x + 1)");
        assertFails(
                MUST_BE_COLUMN_REFERENCE,
                "\\Qline 1:50: GROUP BY expression must be a column reference: (x + 1)\\E",
                "SELECT 1 FROM (VALUES 1) t(x) GROUP BY CUBE(x, x + 1)");
        assertFails(
                MUST_BE_COLUMN_REFERENCE,
                "\\Qline 1:60: GROUP BY expression must be a column reference: (x + 1)\\E",
                "SELECT 1 FROM (VALUES 1) t(x) GROUP BY GROUPING SETS (x, x + 1)");
    }

    @Test
    public void testSingleGroupingSet()
    {
        // TODO: validate output
        analyze("SELECT SUM(b) FROM t1 GROUP BY ()");
        analyze("SELECT SUM(b) FROM t1 GROUP BY GROUPING SETS (())");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS (a)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b))");
    }

    @Test
    public void testMultipleGroupingSetMultipleColumns()
    {
        // TODO: validate output
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b), (c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY a, b, GROUPING SETS ((c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a), (c, d))");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), ROLLUP (c, d)");
        analyze("SELECT a, SUM(b) FROM t1 GROUP BY GROUPING SETS ((a, b)), CUBE (c, d)");
    }

    @Test
    public void testAggregateWithWildcard()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "line 1:25: Column 1 not in GROUP BY clause", "SELECT * FROM (SELECT a + 1, b FROM t1) t GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "line 1:23: Column 't.a' not in GROUP BY clause", "SELECT * FROM (SELECT a, b FROM t1) t GROUP BY b ORDER BY 1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "line 1:23: Column 'a' not in GROUP BY clause", "SELECT * FROM (SELECT a, b FROM t1) GROUP BY b ORDER BY 1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "line 1:25: Column 1 not in GROUP BY clause", "SELECT * FROM (SELECT a + 1, b FROM t1) GROUP BY b ORDER BY 1");
    }

    @Test
    public void testGroupByCase()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE a WHEN 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE 1 WHEN 2 THEN 0 ELSE a END, count(*) FROM t1");

        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN a = 1 THEN 'a' ELSE 'b' END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN a ELSE 0 END, count(*) FROM t1");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT CASE WHEN true THEN 0 ELSE a END, count(*) FROM t1");
    }

    @Test
    public void testGroupingWithWrongColumnsAndNoGroupBy()
    {
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT a, SUM(b), GROUPING(a, b, c, d) FROM t1 GROUP BY GROUPING SETS ((a, b), (c))");
        assertFails(INVALID_PROCEDURE_ARGUMENTS, "SELECT a, SUM(b), GROUPING(a, b) FROM t1");
    }

    @Test
    public void testUnionDistinctPerformanceWarning()
    {
        WarningCollector warningCollector = analyzeWithWarnings("SELECT a,b,c,d FROM t8 UNION DISTINCT SELECT a,b,c,d FROM t9");
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 1);

        // Ensure warning is the performance warning we expect
        PrestoWarning warning = warnings.get(0);
        assertEquals(warning.getWarningCode(), PERFORMANCE_WARNING.toWarningCode());
        assertTrue(warning.getMessage().startsWith("UNION DISTINCT"));
    }

    @Test
    public void testCountDistinctPerformanceWarning()
    {
        WarningCollector warningCollector = analyzeWithWarnings("SELECT COUNT(DISTINCT a) FROM t1 GROUP BY b");
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 1);

        // Ensure warning is the performance warning we expect
        PrestoWarning warning = warnings.get(0);
        assertEquals(warning.getWarningCode(), PERFORMANCE_WARNING.toWarningCode());
        assertTrue(warning.getMessage().contains("COUNT(DISTINCT xxx)"));
    }

    @Test
    public void testApproxDistinctPerformanceWarning()
    {
        WarningCollector warningCollector = analyzeWithWarnings("SELECT approx_distinct(a) FROM t1 GROUP BY b");
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 0);

        warningCollector = analyzeWithWarnings("SELECT approx_distinct(a, 0.0025E0) FROM t1 GROUP BY b");
        warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 1);

        // Ensure warning is the performance warning we expect
        PrestoWarning warning = warnings.get(0);
        assertEquals(warning.getWarningCode(), PERFORMANCE_WARNING.toWarningCode());
        assertTrue(warning.getMessage().startsWith("approx_distinct"));

        // Ensure warning is only issued for values lower than threshold
        warningCollector = analyzeWithWarnings("SELECT approx_distinct(a, 0.0055E0) FROM t1 GROUP BY b");
        warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 0);
    }

    @Test
    public void testApproxSetPerformanceWarning()
    {
        WarningCollector warningCollector = analyzeWithWarnings("SELECT approx_set(a) FROM t1 GROUP BY b");
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 0);

        warningCollector = analyzeWithWarnings("SELECT approx_set(a, 0.0015E0) FROM t1 GROUP BY b");
        warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 1);

        // Ensure warning is the performance warning we expect
        PrestoWarning warning = warnings.get(0);
        assertEquals(warning.getWarningCode(), PERFORMANCE_WARNING.toWarningCode());
        assertTrue(warning.getMessage().startsWith("approx_set"));

        // Ensure warning is only issued for values lower than threshold
        warningCollector = analyzeWithWarnings("SELECT approx_set(a, 0.0055E0) FROM t1 GROUP BY b");
        warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 0);
    }

    @Test
    public void testApproxDistinctAndApproxSetPerformanceWarning()
    {
        WarningCollector warningCollector = analyzeWithWarnings("SELECT approx_distinct(a, 0.0025E0), approx_set(a, 0.0013E0) FROM t1 GROUP BY b");
        List<PrestoWarning> warnings = warningCollector.getWarnings();
        assertEquals(warnings.size(), 2);

        // Ensure warnings are the performance warnings we expect
        PrestoWarning approxDistinctWarning = warnings.get(0);
        assertEquals(approxDistinctWarning.getWarningCode(), PERFORMANCE_WARNING.toWarningCode());
        assertTrue(approxDistinctWarning.getMessage().startsWith("approx_distinct"));
        PrestoWarning approxSetWarning = warnings.get(1);
        assertEquals(approxSetWarning.getWarningCode(), PERFORMANCE_WARNING.toWarningCode());
        assertTrue(approxSetWarning.getMessage().startsWith("approx_set"));
    }

    @Test
    public void testUnionNoPerformanceWarning()
    {
        // <= 3 fields
        WarningCollector warningCollector = analyzeWithWarnings("SELECT a,b,c FROM t8 UNION DISTINCT SELECT a,b,c FROM t9");
        assertTrue(warningCollector.getWarnings().isEmpty());
        // > 3 fields, no expensive types
        //warningCollector = analyzeWithWarnings("SELECT a,b,c,d FROM t1 UNION DISTINCT SELECT a,b,c,d FROM t1");
        assertTrue(warningCollector.getWarnings().isEmpty());
        // > 3 fields, expensive types, not distinct
        warningCollector = analyzeWithWarnings("SELECT a,b,c,d FROM t8 UNION ALL SELECT a,b,c,d FROM t9");
        assertTrue(warningCollector.getWarnings().isEmpty());
    }

    @Test
    public void testMismatchedUnionQueries()
    {
        assertFails(TYPE_MISMATCH, "SELECT 1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "SELECT a FROM t1 UNION SELECT 'a'");
        assertFails(TYPE_MISMATCH, "(SELECT 1) UNION SELECT 'a'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 1, 2 UNION SELECT 1");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "SELECT 'a' UNION SELECT 'b', 'c'");
        assertFails(MISMATCHED_SET_COLUMN_TYPES, "TABLE t2 UNION SELECT 'a'");
        assertFails(
                TYPE_MISMATCH,
                ".* column 1 in UNION query has incompatible types.*",
                "SELECT 123, 'foo' UNION ALL SELECT 'bar', 999");
        assertFails(
                TYPE_MISMATCH,
                ".* column 2 in UNION query has incompatible types.*",
                "SELECT 123, 123 UNION ALL SELECT 999, 'bar'");
    }

    @Test
    public void testUnionUnmatchedOrderByAttribute()
    {
        assertFails(MISSING_ATTRIBUTE, "TABLE t2 UNION ALL SELECT c, d FROM t1 ORDER BY c");
    }

    @Test
    public void testGroupByComplexExpressions()
    {
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(a IS NOT NULL, 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT IF(CAST(a AS VARCHAR) LIKE 'a', 1, 0) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT a IN (1, 2, 3) FROM t1 GROUP BY b");
        assertFails(MUST_BE_AGGREGATE_OR_GROUP_BY, "SELECT 1 IN (a, 2, 3) FROM t1 GROUP BY b");
    }

    @Test
    public void testNonNumericTableSamplePercentage()
    {
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI ('a')");
        assertFails(NON_NUMERIC_SAMPLE_PERCENTAGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (a + 1)");
    }

    @Test
    public void testTableSampleOutOfRange()
    {
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-1)");
        assertFails(SAMPLE_PERCENTAGE_OUT_OF_RANGE, "SELECT * FROM t1 TABLESAMPLE BERNOULLI (-101)");
    }

    @Test
    public void testCreateTableAsColumns()
    {
        // TODO: validate output
        analyze("CREATE TABLE test(a) AS SELECT 123");
        analyze("CREATE TABLE test(a, b) AS SELECT 1, 2");
        analyze("CREATE TABLE test(a) AS (VALUES 1)");

        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE TABLE test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE TABLE test AS SELECT 1 a, 2 a");
        assertFails(COLUMN_TYPE_UNKNOWN, "CREATE TABLE test AS SELECT null a");
        assertFails(MISMATCHED_COLUMN_ALIASES, 1, 19, "CREATE TABLE test(x) AS SELECT 1, 2");
        assertFails(MISMATCHED_COLUMN_ALIASES, 1, 19, "CREATE TABLE test(x, y) AS SELECT 1");
        assertFails(MISMATCHED_COLUMN_ALIASES, 1, 19, "CREATE TABLE test(x, y) AS (VALUES 1)");
        assertFails(DUPLICATE_COLUMN_NAME, 1, 24, "CREATE TABLE test(abc, AbC) AS SELECT 1, 2");
        assertFails(COLUMN_TYPE_UNKNOWN, 1, 1, "CREATE TABLE test(x) AS SELECT null");
        assertFails(MISSING_ATTRIBUTE, ".*'y' cannot be resolved", "CREATE TABLE test(x) WITH (p1 = y) AS SELECT null");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test(x) WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3') AS SELECT null");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test(x) WITH (p1 = 'p1', \"p1\" = 'p2') AS SELECT null");
    }

    @Test
    public void testCreateTable()
    {
        analyze("CREATE TABLE test (id bigint)");
        analyze("CREATE TABLE test (id bigint) WITH (p1 = 'p1')");

        assertFails(MISSING_ATTRIBUTE, ".*'y' cannot be resolved", "CREATE TABLE test (x bigint) WITH (p1 = y)");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test (id bigint) WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3')");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE TABLE test (id bigint) WITH (p1 = 'p1', \"p1\" = 'p2')");
    }

    @Test
    public void testAnalyze()
    {
        analyze("ANALYZE t1");
        analyze("ANALYZE t1 WITH (p1 = 'p1')");

        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "ANALYZE t1 WITH (p1 = 'p1', p2 = 2, p1 = 'p3')");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "ANALYZE t1 WITH (p1 = 'p1', \"p1\" = 'p2')");
    }

    @Test
    public void testCreateSchema()
    {
        analyze("CREATE SCHEMA test");
        analyze("CREATE SCHEMA test WITH (p1 = 'p1')");

        assertFails(MISSING_ATTRIBUTE, ".*'y' cannot be resolved", "CREATE SCHEMA test WITH (p1 = y)");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE SCHEMA test WITH (p1 = 'p1', p2 = 'p2', p1 = 'p3')");
        assertFails(DUPLICATE_PROPERTY, ".* Duplicate property: p1", "CREATE SCHEMA test WITH (p1 = 'p1', \"p1\" = 'p2')");
    }

    @Test
    public void testCreateViewColumns()
    {
        assertFails(COLUMN_NAME_NOT_SPECIFIED, "CREATE VIEW test AS SELECT 123");
        assertFails(DUPLICATE_COLUMN_NAME, "CREATE VIEW test AS SELECT 1 a, 2 a");
        assertFails(COLUMN_TYPE_UNKNOWN, "CREATE VIEW test AS SELECT null a");
    }

    @Test
    public void testCreateRecursiveView()
    {
        assertFails(VIEW_IS_RECURSIVE, "CREATE OR REPLACE VIEW v1 AS SELECT * FROM v1");
    }

    @Test
    public void testExistingRecursiveView()
    {
        analyze("SELECT * FROM v1 a JOIN v1 b ON a.a = b.a");
        analyze("SELECT * FROM v1 a JOIN (SELECT * from v1) b ON a.a = b.a");
        assertFails(VIEW_ANALYSIS_ERROR, "SELECT * FROM v5");
    }

    @Test
    public void testShowCreateView()
    {
        analyze("SHOW CREATE VIEW v1");
        analyze("SHOW CREATE VIEW v2");

        assertFails(NOT_SUPPORTED, "SHOW CREATE VIEW t1");
        assertFails(MISSING_TABLE, "SHOW CREATE VIEW none");
    }

    @Test
    public void testStaleView()
    {
        assertFails(VIEW_IS_STALE, "SELECT * FROM v2");
    }

    @Test
    public void testStoredViewAnalysisScoping()
    {
        // the view must not be analyzed using the query context
        analyze("WITH t1 AS (SELECT 123 x) SELECT * FROM v1");
    }

    @Test
    public void testStoredViewResolution()
    {
        // the view must be analyzed relative to its own catalog/schema
        analyze("SELECT * FROM c3.s3.v3");
    }

    @Test
    public void testQualifiedViewColumnResolution()
    {
        // it should be possible to qualify the column reference with the view name
        analyze("SELECT v1.a FROM v1");
        analyze("SELECT s1.v1.a FROM s1.v1");
        analyze("SELECT tpch.s1.v1.a FROM tpch.s1.v1");
    }

    @Test
    public void testViewWithUppercaseColumn()
    {
        analyze("SELECT * FROM v4");
    }

    @Test
    public void testUse()
    {
        assertFails(NOT_SUPPORTED, "USE foo");
    }

    @Test
    public void testNotNullInJoinClause()
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON a.x IS NOT NULL");
    }

    @Test
    public void testIfInJoinClause()
    {
        analyze("SELECT * FROM (VALUES (1)) a (x) JOIN (VALUES (2)) b ON IF(a.x = 1, true, false)");
    }

    @Test
    public void testLiteral()
    {
        assertFails(INVALID_LITERAL, "SELECT TIMESTAMP '2012-10-31 01:00:00 PT'");
    }

    @Test
    public void testLambda()
    {
        analyze("SELECT apply(5, x -> abs(x)) from t1");
        assertFails(STANDALONE_LAMBDA, "SELECT x -> abs(x) from t1");
    }

    @Test
    public void testLambdaCapture()
    {
        analyze("SELECT apply(c1, x -> x + c2) FROM (VALUES (1, 2), (3, 4), (5, 6)) t(c1, c2)");
        analyze("SELECT apply(c1 + 10, x -> apply(x + 100, y -> c1)) FROM (VALUES 1) t(c1)");

        // reference lambda variable of the not-immediately-enclosing lambda
        analyze("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 1000) t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> x)) FROM (VALUES 'abc') t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 1000) t(x)");
        analyze("SELECT apply(1, x -> apply(10, y -> apply(100, z -> x))) FROM (VALUES 'abc') t(x)");
    }

    @Test
    public void testLambdaInAggregationContext()
    {
        analyze("SELECT apply(sum(x), i -> i * i) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        analyze("SELECT apply(x, i -> i - 1), sum(y) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
        analyze("SELECT x, apply(sum(y), i -> i * 10) FROM (VALUES (1, 10), (1, 20), (2, 50)) t(x,y) group by x");
        analyze("SELECT apply(8, x -> x + 1) FROM (VALUES (1, 2)) t(x,y) GROUP BY y");

        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(sum(x), i -> i * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, y -> x) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, y -> x.someField) FROM (VALUES (CAST(ROW(1) AS ROW(someField BIGINT)), 2)) t(x,y) GROUP BY y");
        analyze("SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> x.someField) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        analyze("SELECT apply(sum(x), x -> x * x) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // nested lambda expression uses the same variable name
        analyze("SELECT apply(sum(x), x -> apply(x, x -> x * x)) FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // illegal use of a column whose name is the same as a lambda variable name
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(sum(x), x -> x * x) + x FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(sum(x), x -> apply(x, x -> x * x)) + x FROM (VALUES 1, 2, 3, 4, 5) t(x)");
        // x + y within lambda should not be treated as group expression
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                ".* must be an aggregate expression or appear in GROUP BY clause",
                "SELECT apply(1, y -> x + y) FROM (VALUES (1,2)) t(x, y) GROUP BY x+y");
    }

    @Test
    public void testLambdaInSubqueryContext()
    {
        analyze("SELECT apply(x, i -> i * i) FROM (SELECT 10 x)");
        analyze("SELECT apply((SELECT 10), i -> i * i)");

        // with capture
        analyze("SELECT apply(x, i -> i * x) FROM (SELECT 10 x)");
        analyze("SELECT apply(x, y -> y * x) FROM (SELECT 10 x, 3 y)");
        analyze("SELECT apply(x, z -> y * x) FROM (SELECT 10 x, 3 y)");
    }

    @Test
    public void testLambdaWithAggregationAndGrouping()
    {
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT transform(ARRAY[1], y -> max(x)) FROM (VALUES 10) t(x)");

        // use of aggregation/window function on lambda variable
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT apply(1, x -> max(x)) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT apply(CAST(ROW(1) AS ROW(someField BIGINT)), x -> max(x.someField)) FROM (VALUES (1,2)) t(x,y) GROUP BY y");
        assertFails(
                CANNOT_HAVE_AGGREGATIONS_WINDOWS_OR_GROUPING,
                ".* Lambda expression cannot contain aggregations, window functions or grouping operations: .*",
                "SELECT apply(1, x -> grouping(x)) FROM (VALUES (1, 2)) t(x, y) GROUP BY y");
    }

    @Test
    public void testLambdaWithSubquery()
    {
        assertFails(
                NOT_SUPPORTED,
                ".* Lambda expression cannot contain subqueries",
                "SELECT apply(1, i -> (SELECT 3)) FROM (VALUES 1) t(x)");
        assertFails(
                NOT_SUPPORTED,
                ".* Lambda expression cannot contain subqueries",
                "SELECT apply(1, i -> (SELECT i)) FROM (VALUES 1) t(x)");

        // GROUP BY column captured in lambda
        analyze(
                "SELECT (SELECT apply(0, x -> x + b) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");

        // non-GROUP BY column captured in lambda
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:34: Subquery uses 'a' which must appear in GROUP BY clause",
                "SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(c)) " +
                        "FROM t1 u GROUP BY b");
        // TODO #7784
//        assertFails(
//                MUST_BE_AGGREGATE_OR_GROUP_BY,
//                "line 1:34: Subquery uses '\"u.a\"' which must appear in GROUP BY clause",
//                "SELECT (SELECT apply(0, x -> x + u.a) from (values 1) x(a)) " +
//                        "FROM t1 u GROUP BY b");

        // name shadowing
        analyze("SELECT (SELECT apply(0, x -> x + a) FROM (VALUES 1) x(a)) FROM t1 u GROUP BY b");
        analyze("SELECT (SELECT apply(0, a -> a + a)) FROM t1 u GROUP BY b");
    }

    @Test
    public void testLambdaWithSubqueryInOrderBy()
    {
        analyze("SELECT a FROM t1 ORDER BY (SELECT apply(0, x -> x + a))");
        analyze("SELECT a AS output_column FROM t1 ORDER BY (SELECT apply(0, x -> x + output_column))");
        analyze("SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + a))");
        analyze("SELECT count(*) AS output_column FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + output_column))");
        assertFails(
                MUST_BE_AGGREGATE_OR_GROUP_BY,
                "line 1:71: Subquery uses 'b' which must appear in GROUP BY clause",
                "SELECT count(*) FROM t1 GROUP BY a ORDER BY (SELECT apply(0, x -> x + b))");
    }

    @Test
    public void testLambdaWithInvalidParameterCount()
    {
        assertFails(INVALID_PARAMETER_USAGE, "line 1:17: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT apply(5, (x, y) -> 6)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:17: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT apply(5, (x, y, z) -> 6)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:21: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT TRY(apply(5, (x, y) -> x + 1) / 0)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:21: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT TRY(apply(5, (x, y, z) -> x + 1) / 0)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:29: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT filter(ARRAY [5, 6], (x, y) -> x = 5)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:29: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT filter(ARRAY [5, 6], (x, y, z) -> x = 5)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT map_filter(map(ARRAY [5, 6], ARRAY [5, 6]), (x) -> x = 1)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT map_filter(map(ARRAY [5, 6], ARRAY [5, 6]), (x, y, z) -> x = y + z)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:33: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT reduce(ARRAY [5, 20], 0, (s) -> s, s -> s)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:33: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT reduce(ARRAY [5, 20], 0, (s, x, z) -> s + x, s -> s + z)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:32: Expected a lambda that takes 1 argument\\(s\\) but got 2", "SELECT transform(ARRAY [5, 6], (x, y) -> x + y)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:32: Expected a lambda that takes 1 argument\\(s\\) but got 3", "SELECT transform(ARRAY [5, 6], (x, y, z) -> x + y + z)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:49: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT transform_keys(map(ARRAY[1], ARRAY [2]), k -> k)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:52: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT transform_keys(MAP(ARRAY['a'], ARRAY['b']), (k, v, x) -> k + 1)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:51: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT transform_values(map(ARRAY[1], ARRAY [2]), k -> k)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:51: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT transform_values(map(ARRAY[1], ARRAY [2]), (k, v, x) -> k + 1)");

        assertFails(INVALID_PARAMETER_USAGE, "line 1:39: Expected a lambda that takes 2 argument\\(s\\) but got 1", "SELECT zip_with(ARRAY[1], ARRAY['a'], x -> x)");
        assertFails(INVALID_PARAMETER_USAGE, "line 1:39: Expected a lambda that takes 2 argument\\(s\\) but got 3", "SELECT zip_with(ARRAY[1], ARRAY['a'], (x, y, z) -> (x, y, z))");
    }

    @Test
    public void testInvalidDelete()
    {
        assertFails(MISSING_TABLE, "DELETE FROM foo");
        assertFails(NOT_SUPPORTED, "DELETE FROM v1");
        assertFails(NOT_SUPPORTED, "DELETE FROM v1 WHERE a = 1");
    }

    @Test
    public void testInvalidShowTables()
    {
        assertFails(INVALID_SCHEMA_NAME, "SHOW TABLES FROM a.b.c");

        Session session = testSessionBuilder()
                .setCatalog(null)
                .setSchema(null)
                .build();
        assertFails(session, CATALOG_NOT_SPECIFIED, "SHOW TABLES");
        assertFails(session, CATALOG_NOT_SPECIFIED, "SHOW TABLES FROM a");
        assertFails(session, MISSING_SCHEMA, "SHOW TABLES FROM c2.unknown");

        session = testSessionBuilder()
                .setCatalog(SECOND_CATALOG)
                .setSchema(null)
                .build();
        assertFails(session, SCHEMA_NOT_SPECIFIED, "SHOW TABLES");
        assertFails(session, MISSING_SCHEMA, "SHOW TABLES FROM unknown");
    }

    @Test
    public void testInvalidAtTimeZone()
    {
        assertFails(TYPE_MISMATCH, "SELECT 'abc' AT TIME ZONE 'America/Los_Angeles'");
    }

    @Test
    public void testValidJoinOnClause()
    {
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON TRUE");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1=1");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x=b.x AND a.y=b.y");
        analyze("SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON NULL");
    }

    @Test
    public void testInValidJoinOnClause()
    {
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON 1");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON a.x + b.x");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON ROW (TRUE)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM (VALUES (2, 2)) a(x,y) JOIN (VALUES (2, 2)) b(x,y) ON (a.x=b.x, a.y=b.y)");
    }

    @Test
    public void testInvalidAggregationFilter()
    {
        assertFails(NOT_SUPPORTED, "SELECT sum(x) FILTER (WHERE x > 1) OVER (PARTITION BY x) FROM (VALUES (1), (2), (2), (4)) t (x)");
        assertFails(MUST_BE_AGGREGATION_FUNCTION, "SELECT abs(x) FILTER (where y = 1) FROM (VALUES (1, 1)) t(x, y)");
        assertFails(MUST_BE_AGGREGATION_FUNCTION, "SELECT abs(x) FILTER (where y = 1) FROM (VALUES (1, 1, 1)) t(x, y, z) GROUP BY z");
    }

    @Test
    void testAggregationWithOrderBy()
    {
        analyze("SELECT array_agg(DISTINCT x ORDER BY x) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        analyze("SELECT array_agg(x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails(ORDER_BY_MUST_BE_IN_AGGREGATE, "SELECT array_agg(DISTINCT x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails(MUST_BE_AGGREGATION_FUNCTION, "SELECT abs(x ORDER BY y) FROM (VALUES (1, 2), (3, 4)) t(x, y)");
        assertFails(TYPE_MISMATCH, "SELECT array_agg(x ORDER BY x) FROM (VALUES MAP(ARRAY['a'], ARRAY['b'])) t(x)");
        assertFails(MISSING_ATTRIBUTE, "SELECT 1 as a, array_agg(x ORDER BY a) FROM (VALUES (1), (2), (3)) t(x)");
        assertFails(REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION, "SELECT 1 AS c FROM (VALUES (1), (2)) t(x) ORDER BY sum(x order by c)");
    }

    @Test
    public void testQuantifiedComparisonExpression()
    {
        analyze("SELECT * FROM t1 WHERE t1.a <= ALL (VALUES 10, 20)");
        assertFails(MULTIPLE_FIELDS_FROM_SUBQUERY, "SELECT * FROM t1 WHERE t1.a = ANY (SELECT 1, 2)");
        assertFails(TYPE_MISMATCH, "SELECT * FROM t1 WHERE t1.a = SOME (VALUES ('abc'))");

        // map is not orderable
        assertFails(TYPE_MISMATCH, ("SELECT map(ARRAY[1], ARRAY['hello']) < ALL (VALUES map(ARRAY[1], ARRAY['hello']))"));
        // but map is comparable
        analyze(("SELECT map(ARRAY[1], ARRAY['hello']) = ALL (VALUES map(ARRAY[1], ARRAY['hello']))"));

        // HLL is neither orderable nor comparable
        assertFails(TYPE_MISMATCH, "SELECT cast(NULL AS HyperLogLog) < ALL (VALUES cast(NULL AS HyperLogLog))");
        assertFails(TYPE_MISMATCH, "SELECT cast(NULL AS HyperLogLog) = ANY (VALUES cast(NULL AS HyperLogLog))");

        // qdigest is neither orderable nor comparable
        assertFails(TYPE_MISMATCH, "SELECT cast(NULL AS qdigest(double)) < ALL (VALUES cast(NULL AS qdigest(double)))");
        assertFails(TYPE_MISMATCH, "SELECT cast(NULL AS qdigest(double)) = ANY (VALUES cast(NULL AS qdigest(double)))");
    }

    @Test
    public void testJoinUnnest()
    {
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN UNNEST(x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN UNNEST(x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN UNNEST(x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN UNNEST(x) ON true");
    }

    @Test
    public void testUnnestInnerScalarAlias()
    {
        analyze("SELECT * FROM (SELECT array[1,2] a) a CROSS JOIN UNNEST(a) AS T(x)");
    }

    @Test(expectedExceptions = SemanticException.class, expectedExceptionsMessageRegExp = "line 1:37: Scalar subqueries in UNNEST are not supported")
    public void testUnnestInnerScalar()
    {
        analyze("SELECT * FROM (SELECT 1) CROSS JOIN UNNEST((SELECT array[1])) AS T(x)");
    }

    @Test
    public void testJoinLateral()
    {
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) CROSS JOIN LATERAL(VALUES x)");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) LEFT OUTER JOIN LATERAL(VALUES x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) RIGHT OUTER JOIN LATERAL(VALUES x) ON true");
        analyze("SELECT * FROM (VALUES array[2, 2]) a(x) FULL OUTER JOIN LATERAL(VALUES x) ON true");
    }

    @Test
    public void testEmptyTableName()
    {
        assertFails(MISSING_TABLE, "SELECT * FROM \"\"");
    }

    @Test
    public void testEmptySchemaName()
    {
        assertFails(MISSING_SCHEMA, "SELECT * FROM \"\".foo");
    }

    @Test
    public void testReplaceTemporaryFunctionFails()
    {
        assertFails(NOT_SUPPORTED, "CREATE OR REPLACE TEMPORARY FUNCTION foo() RETURNS INT RETURN 1");
    }

    @Test
    public void testJoinOnPerformatestJoinOnPerformanceWarningsnceWarnings()
    {
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t1.b"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't2' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t1.a = t2.a"),
                PERFORMANCE_WARNING, "line 1:67: JOIN ON condition(s) do not reference the joined table 't3' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("select * FROM t1 a1 LEFT JOIN t2 a2 ON a1.a = a2.a LEFT JOIN t3 a3  ON a1.a = a2.a"),
                PERFORMANCE_WARNING, "line 1:77: JOIN ON condition(s) do not reference the joined table 't3' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 a1 LEFT JOIN t2 a2 ON a1.a = a2.a LEFT JOIN t3 a3  ON a3.a = a1.a"));
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t3.a = t1.a"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t3.a = 1"),
                PERFORMANCE_WARNING, "line 1:67: JOIN ON condition(s) do not reference the joined table 't3' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t1.a = 1 AND t3.a = t1.a"));
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a LEFT JOIN t3 ON t3.a = t1.a"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 a1 LEFT JOIN t2 a2 ON a1.a = a2.a LEFT JOIN t3 a3  ON a3.a = a3.b"),
                PERFORMANCE_WARNING, "line 1:77: JOIN ON condition(s) do not reference the joined table 't3' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 a1 LEFT JOIN t2 a2 ON a1.a = a2.a LEFT JOIN t3 a3  ON a3.a = a3.b AND a3.b = a1.b"));
        assertHasWarning(analyzeWithWarnings("select * from t1 inner join ( select t2.a as t2_a, t3.a from t2 inner join t3 on t2.a=t3.a) al ON t1.a = t1.a"),
                PERFORMANCE_WARNING, "line 1:104: JOIN ON condition(s) do not reference the joined relation and other relation in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertHasWarning(analyzeWithWarnings("select * from t1 inner join (select t2.a as t2_a, t3.a t3_a from t2 inner join t3 on t2.a=t3.a) ON t2_a = t3_a"),
                PERFORMANCE_WARNING, "line 1:105: JOIN ON condition(s) do not reference the joined relation and other relation in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * from t1 inner join (select t2.a as t2_a, t3.a t3_a from t2 inner join t3 on t2.a=t3.a) ON t2_a = a"));
        assertNoWarning(analyzeWithWarnings("select * from (select ARBITRARY(a) aa from t1 group by b) _ join (select w from t13)  ON w = aa"));
        assertNoWarning(analyzeWithWarnings("with tt1 as (select a,b from t1) select b,x from t13 JOIN tt1 ON w = a"));
        assertHasWarning(analyzeWithWarnings("with tt1 as (select a,b from t1) select b,x from t13 JOIN tt1 ON x = y"),
                PERFORMANCE_WARNING, "line 1:68: JOIN ON condition(s) do not reference the joined table 'tt1' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a + 1"));
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a + t2.a = t1.b + t2.b"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t1.b + 1"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't2' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = t2.a * (t2.b + 1)"));
        assertHasWarning(analyzeWithWarnings("select * FROM t2 LEFT JOIN t1 ON t1.c = t1.a * (t1.b + 1)"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't1' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = -t2.a"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = -t1.b"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't2' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t2.a = COALESCE(t1.a, t1.b, 1)"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t2.a = COALESCE(t2.a, t2.b, 1)"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't2' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t2.a = CASE WHEN t1.a > 0 THEN 1 WHEN t1.a < 0 THEN -1 WHEN t1.a = 0 THEN 0 END"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t1.a = CASE WHEN t1.a > 0 THEN 1 WHEN t1.a < 0 THEN -1 WHEN t1.a = 0 THEN 0 END"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't2' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t2.a BETWEEN t1.a AND t1.b"));
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t2.a BETWEEN t2.a AND t1.b"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t2 ON t2.a BETWEEN t2.a AND t2.b"),
                PERFORMANCE_WARNING, "line 1:39: JOIN ON condition(s) do not reference the joined table 't2' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t6 LEFT JOIN t6 as t6_1 ON hamming_distance(t6.b, t6_1.b) > 0"));
        assertHasWarning(analyzeWithWarnings("select * FROM t6 LEFT JOIN t6 as t6_1 ON hamming_distance(t6.b, t6.b) > 0"),
                PERFORMANCE_WARNING, "line 1:71: JOIN ON condition(s) do not reference the joined table 't6' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
        assertNoWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t10 ON t1.a = t10.b.x.z"));
        assertHasWarning(analyzeWithWarnings("select * FROM t1 LEFT JOIN t10 ON t10.a = t10.b.x.z"),
                PERFORMANCE_WARNING, "line 1:41: JOIN ON condition(s) do not reference the joined table 't10' and other tables in the same expression that can cause " +
                        "performance issues as it may lead to a cross join with filter");
    }

    @Test
    public void testInvalidTemporaryFunctionName()
    {
        assertFails(INVALID_FUNCTION_NAME, "CREATE TEMPORARY FUNCTION sum() RETURNS INT RETURN 1");
        assertFails(INVALID_FUNCTION_NAME, "CREATE TEMPORARY FUNCTION dev.test.foo() RETURNS INT RETURN 1");
    }

    @Test
    public void testTableFunctionNotFound()
    {
        assertFails(FUNCTION_NOT_FOUND,
                "line 1:21: Table function non_existent_table_function not registered",
                "SELECT * FROM TABLE(non_existent_table_function())");
    }

    @Test
    public void testTableFunctionArguments()
    {
        assertFails(TABLE_FUNCTION_INVALID_ARGUMENTS, "line 1:58: Too many arguments. Expected at most 2 arguments, got 3 arguments", "SELECT * FROM TABLE(system.two_scalar_arguments_function(1, 2, 3))");

        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function('foo'))");
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'foo'))");
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function('foo', 1))");
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'foo', number => 1))");

        assertFails(TABLE_FUNCTION_INVALID_ARGUMENTS,
                "line 1:58: All arguments must be passed by name or all must be passed positionally",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function('foo', number => 1))");

        assertFails(TABLE_FUNCTION_INVALID_ARGUMENTS,
                "line 1:58: All arguments must be passed by name or all must be passed positionally",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'foo', 1))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:73: Duplicate argument name: TEXT",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'foo', text => 'bar'))");

        // argument names are resolved in the canonical form
        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:73: Duplicate argument name: TEXT",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'foo', TeXt => 'bar'))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:73: Unexpected argument name: BAR",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'foo', bar => 'bar'))");

        assertFails(TABLE_FUNCTION_MISSING_ARGUMENT,
                "line 1:58: Missing argument: TEXT",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(number => 1))");
    }

    @Test
    public void testScalarArgument()
    {
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function('foo', 1))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:71: Invalid argument NUMBER. Expected expression, got descriptor",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'a', number => DESCRIPTOR(x integer, y boolean)))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:71: 'descriptor' function is not allowed as a table function argument",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'a', number => DESCRIPTOR(1 + 2)))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:71: Invalid argument NUMBER. Expected expression, got table",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'a', number => TABLE(t1)))");

        assertFails(EXPRESSION_NOT_CONSTANT,
                "line 1:81: Constant expression cannot contain a subquery",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function(text => 'a', number => (SELECT 1)))");
    }

    @Test
    public void testTableArgument()
    {
        // cannot pass a table function as the argument
        assertFails(NOT_SUPPORTED,
                "line 1:52: Invalid table argument INPUT. Table functions are not allowed as table function arguments",
                "SELECT * FROM TABLE(system.table_argument_function(input => my_schema.my_table_function(1)))");

        assertThatThrownBy(() -> analyze("SELECT * FROM TABLE(system.table_argument_function(input => my_schema.my_table_function(arg => 1)))"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("line 1:93: mismatched input '=>'.");

        // cannot pass a table function as the argument, also preceding nested table function with TABLE is incorrect
        assertThatThrownBy(() -> analyze("SELECT * FROM TABLE(system.table_argument_function(input => TABLE(my_schema.my_table_function(1))))"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("line 1:94: mismatched input '('.");

        // a table passed as the argument must be preceded with TABLE
        analyze("SELECT * FROM TABLE(system.table_argument_function(input => TABLE(t1)))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:52: Invalid argument INPUT. Expected table, got expression",
                "SELECT * FROM TABLE(system.table_argument_function(input => t1))");

        // a query passed as the argument must be preceded with TABLE
        analyze("SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT * FROM t1)))");

        assertThatThrownBy(() -> analyze("SELECT * FROM TABLE(system.table_argument_function(input => SELECT * FROM t1))"))
                .isInstanceOf(ParsingException.class)
                .hasMessageContaining("line 1:61: mismatched input 'SELECT'.");

        // query passed as the argument is correlated
        analyze("SELECT * FROM t1 CROSS JOIN LATERAL (SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT 1 WHERE a > 0))))");

        // wrong argument type
        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:52: Invalid argument INPUT. Expected table, got expression",
                "SELECT * FROM TABLE(system.table_argument_function(input => 'foo'))");
        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:52: Invalid argument INPUT. Expected table, got descriptor",
                "SELECT * FROM TABLE(system.table_argument_function(input => DESCRIPTOR(x int, y int)))");
    }

    @Test
    public void testTableArgumentProperties()
    {
        analyze("SELECT * FROM TABLE(system.table_argument_function(input => TABLE(t1) PARTITION BY a KEEP WHEN EMPTY ORDER BY b))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:66: Invalid argument INPUT. Partitioning specified for table argument with row semantics",
                "SELECT * FROM TABLE(system.table_argument_row_semantics_function(input => TABLE(t1) PARTITION BY a))");

        assertFails(TABLE_FUNCTION_COLUMN_NOT_FOUND,
                "line 1:92: Column b is not present in the input relation",
                "SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT 1 a) PARTITION BY b))");

        assertFails(TABLE_FUNCTION_INVALID_COLUMN_REFERENCE,
                "line 1:88: Expected column reference. Actual: 1",
                "SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT 1 a) ORDER BY 1))");

        assertFails(TYPE_MISMATCH,
                "line 1:104: HyperLogLog is not comparable, and therefore cannot be used in PARTITION BY",
                "SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT approx_set(1) a) PARTITION BY a))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT, "line 1:66: Invalid argument INPUT. Ordering specified for table argument with row semantics",
                "SELECT * FROM TABLE(system.table_argument_row_semantics_function(input => TABLE(t1) ORDER BY a))");

        assertFails(TABLE_FUNCTION_COLUMN_NOT_FOUND,
                "line 1:88: Column b is not present in the input relation",
                "SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT 1 a) ORDER BY b))");

        assertFails(TABLE_FUNCTION_INVALID_COLUMN_REFERENCE,
                "line 1:88: Expected column reference. Actual: 1",
                "SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT 1 a) ORDER BY 1))");

        assertFails(TYPE_MISMATCH,
                "line 1:100: HyperLogLog is not orderable, and therefore cannot be used in ORDER BY",
                "SELECT * FROM TABLE(system.table_argument_function(input => TABLE(SELECT approx_set(1) a) ORDER BY a))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:85: Invalid argument INPUT. Empty behavior specified for table argument with row semantics",
                "SELECT * FROM TABLE(system.table_argument_row_semantics_function(input => TABLE(t1) PRUNE WHEN EMPTY))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:85: Invalid argument INPUT. Empty behavior specified for table argument with row semantics",
                "SELECT * FROM TABLE(system.table_argument_row_semantics_function(input => TABLE(t1) KEEP WHEN EMPTY))");
    }

    @Test
    public void testDescriptorArgument()
    {
        analyze("SELECT * FROM TABLE(system.descriptor_argument_function(schema => DESCRIPTOR(x integer, y boolean)))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                Pattern.quote("line 1:57: Invalid descriptor argument SCHEMA. Descriptors should be formatted as 'DESCRIPTOR(name [type], ...)'"),
                "SELECT * FROM TABLE(system.descriptor_argument_function(schema => DESCRIPTOR(1 + 2)))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:57: Invalid argument SCHEMA. Expected descriptor, got expression",
                "SELECT * FROM TABLE(system.descriptor_argument_function(schema => 1))");

        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:57: Invalid argument SCHEMA. Expected descriptor, got table",
                "SELECT * FROM TABLE(system.descriptor_argument_function(schema => TABLE(t1)))");

        assertFails(TYPE_MISMATCH,
                "line 1:78: Unknown type: verybigint",
                "SELECT * FROM TABLE(system.descriptor_argument_function(schema => DESCRIPTOR(x verybigint)))");
    }

    @Test
    public void testCopartitioning()
    {
        // TABLE(t1) is matched by fully qualified name: tpch.s1.t1. It matches the second copartition item s1.t1.
        // Aliased relation TABLE(SELECT 1, 2) t1(x, y) is matched by unqualified name. It matches the first copartition item t1.
        analyze("SELECT * FROM TABLE(system.two_table_arguments_function(" +
                "input1 => TABLE(t1) PARTITION BY (a, b)," +
                "input2 => TABLE(SELECT 1, 2) t1(x, y) PARTITION BY (x, y)" +
                "COPARTITION (t1, s1.t1)))");

        // Copartition items t1, t2 are first matched to arguments by unqualified names, and when no match is found, by fully qualified names.
        // TABLE(tpch.s1.t1) is matched by fully qualified name. It matches the first copartition item t1.
        // TABLE(s1.t2) is matched by unqualified name: tpch.s1.t2. It matches the second copartition item t2.
        analyze("SELECT * FROM TABLE(system.two_table_arguments_function(" +
                "input1 => TABLE(tpch.s1.t1) PARTITION BY (a, b)," +
                "input2 => TABLE(s1.t2) PARTITION BY (a, b)" +
                "COPARTITION (t1, t2)))");

        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING,
                "line 1:153: No table argument found for name: s1.foo",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(t1) PARTITION BY (a, b)," +
                        "input2 => TABLE(t2) PARTITION BY (a, b)" +
                        "COPARTITION (t1, s1.foo)))");

        // Both table arguments are matched by fully qualified name: tpch.s1.t1
        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING, "line 1:149: Ambiguous reference: multiple table arguments found for name: t1",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(t1) PARTITION BY (a, b)," +
                        "input2 => TABLE(t1) PARTITION BY (a, b)" +
                        "COPARTITION (t1, t2)))");

        // Both table arguments are matched by unqualified name: t1
        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING,
                "line 1:185: Ambiguous reference: multiple table arguments found for name: t1",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(SELECT 1, 2) t1(a, b) PARTITION BY (a, b)," +
                        "input2 => TABLE(SELECT 3, 4) t1(c, d) PARTITION BY (c, d)" +
                        "COPARTITION (t1, t2)))");

        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING,
                "line 1:153: Multiple references to table argument: t1 in COPARTITION clause",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(t1) PARTITION BY (a, b)," +
                        "input2 => TABLE(t2) PARTITION BY (a, b)" +
                        "COPARTITION (t1, t1)))");
    }

    @Test
    public void testCopartitionColumns()
    {
        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING,
                "line 1:67: Table tpch.s1.t1 referenced in COPARTITION clause is not partitioned",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(t1)," +
                        "input2 => TABLE(t2) PARTITION BY (a, b)" +
                        "COPARTITION (t1, t2)))");

        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING,
                "line 1:67: No partitioning columns specified for table tpch.s1.t1 referenced in COPARTITION clause",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(t1) PARTITION BY ()," +
                        "input2 => TABLE(t2) PARTITION BY ()" +
                        "COPARTITION (t1, t2)))");

        assertFails(TABLE_FUNCTION_INVALID_COPARTITIONING,
                "line 1:146: Numbers of partitioning columns in copartitioned tables do not match",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(t1) PARTITION BY (a, b)," +
                        "input2 => TABLE(t2) PARTITION BY (a)" +
                        "COPARTITION (t1, t2)))");

        assertFails(TYPE_MISMATCH,
                "line 1:169: Partitioning columns in copartitioned tables have incompatible types",
                "SELECT * FROM TABLE(system.two_table_arguments_function(" +
                        "input1 => TABLE(SELECT 1) t1(a) PARTITION BY (a)," +
                        "input2 => TABLE(SELECT 'x') t2(b) PARTITION BY (b)" +
                        "COPARTITION (t1, t2)))");
    }

    @Test
    public void testNullArguments()
    {
        // cannot pass null for table argument
        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:52: Invalid argument INPUT. Expected table, got expression",
                "SELECT * FROM TABLE(system.table_argument_function(input => null))");

        // the wrong way to pass null for descriptor
        assertFails(TABLE_FUNCTION_INVALID_FUNCTION_ARGUMENT,
                "line 1:57: Invalid argument SCHEMA. Expected descriptor, got expression",
                "SELECT * FROM TABLE(system.descriptor_argument_function(schema => null))");

        // the right way to pass null for descriptor
        analyze("SELECT * FROM TABLE(system.descriptor_argument_function(schema => CAST(null AS DESCRIPTOR)))");

        // the default value for the argument schema is null
        analyze("SELECT * FROM TABLE(system.descriptor_argument_function())");

        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function(null, null))");

        // the default value for the second argument is null
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function('a'))");
    }

    @Test
    public void testTableFunctionInvocationContext()
    {
        // cannot specify relation alias for table function with ONLY PASS THROUGH return type
        assertFails(TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION,
                "line 1:21: Alias specified for table function with ONLY PASS THROUGH return type",
                "SELECT * FROM TABLE(system.only_pass_through_function(TABLE(t1))) f(x)");

        // per SQL standard, relation alias is required for table function with GENERIC TABLE return type. We don't require it.
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function('a', 1)) f(x)");
        analyze("SELECT * FROM TABLE(system.two_scalar_arguments_function('a', 1))");

        // per SQL standard, relation alias is required for table function with statically declared return type, only if the function is polymorphic.
        // We don't require aliasing polymorphic functions.
        analyze("SELECT * FROM TABLE(system.monomorphic_static_return_type_function())");
        analyze("SELECT * FROM TABLE(system.monomorphic_static_return_type_function()) f(x, y)");
        analyze("SELECT * FROM TABLE(system.polymorphic_static_return_type_function(input => TABLE(t1)))");
        analyze("SELECT * FROM TABLE(system.polymorphic_static_return_type_function(input => TABLE(t1))) f(x, y)");

        // sampled
        assertFails(TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION,
                "line 1:21: Cannot apply sample to polymorphic table function invocation",
                "SELECT * FROM TABLE(system.only_pass_through_function(TABLE(t1))) TABLESAMPLE BERNOULLI (10)");

        // aliased + sampled
        assertFails(TABLE_FUNCTION_INVALID_TABLE_FUNCTION_INVOCATION,
                "line 1:15: Cannot apply sample to polymorphic table function invocation",
                "SELECT * FROM TABLE(system.two_scalar_arguments_function('a', 1)) f(x) TABLESAMPLE BERNOULLI (10)");
    }

    @Test
    public void testTableFunctionAliasing()
    {
        // case-insensitive name matching
        assertFails(TABLE_FUNCTION_DUPLICATE_RANGE_VARIABLE,
                "line 1:64: Relation alias: T1 is a duplicate of input table name: tpch.s1.t1",
                "SELECT * FROM TABLE(system.table_argument_function(TABLE(t1))) T1(x)");

        assertFails(TABLE_FUNCTION_DUPLICATE_RANGE_VARIABLE,
                "line 1:76: Relation alias: t1 is a duplicate of input table name: t1",
                "SELECT * FROM TABLE(system.table_argument_function(TABLE(SELECT 1) T1(a))) t1(x)");

        analyze("SELECT * FROM TABLE(system.table_argument_function(TABLE(t1) t2)) T1(x)");

        // the original returned relation type is ("column" : BOOLEAN)
        analyze("SELECT column FROM TABLE(system.two_scalar_arguments_function('a', 1)) table_alias");

        analyze("SELECT column_alias FROM TABLE(system.two_scalar_arguments_function('a', 1)) table_alias(column_alias)");

        analyze("SELECT table_alias.column_alias FROM TABLE(system.two_scalar_arguments_function('a', 1)) table_alias(column_alias)");

        assertFails(MISSING_ATTRIBUTE,
                "line 1:8: Column 'column' cannot be resolved",
                "SELECT column FROM TABLE(system.two_scalar_arguments_function('a', 1)) table_alias(column_alias)");

        assertFails(MISMATCHED_COLUMN_ALIASES,
                "line 1:20: Column alias list has 3 entries but table function has 1 proper columns",
                "SELECT column FROM TABLE(system.two_scalar_arguments_function('a', 1)) table_alias(col1, col2, col3)");

        // the original returned relation type is ("a" : BOOLEAN, "b" : INTEGER)
        analyze("SELECT column_alias_1, column_alias_2 FROM TABLE(system.monomorphic_static_return_type_function()) table_alias(column_alias_1, column_alias_2)");

        assertFails(DUPLICATE_COLUMN_NAME,
                "line 1:21: Duplicate name of table function proper column: col",
                "SELECT * FROM TABLE(system.monomorphic_static_return_type_function()) table_alias(col, col)");

        // case-insensitive name matching
        assertFails(DUPLICATE_COLUMN_NAME,
                "line 1:21: Duplicate name of table function proper column: col",
                "SELECT * FROM TABLE(system.monomorphic_static_return_type_function()) table_alias(col, COL)");

        // pass-through columns of an input table must not be aliased, and must be referenced by the original range variables of their corresponding table arguments
        // the function pass_through_function has one proper column ("x" : BOOLEAN), and one table argument with pass-through property
        // tha alias applies only to the proper column
        analyze("SELECT table_alias.x, t1.a, t1.b, t1.c, t1.d FROM TABLE(system.pass_through_function(TABLE(t1))) table_alias");

        analyze("SELECT table_alias.x, arg_alias.a, arg_alias.b, arg_alias.c, arg_alias.d FROM TABLE(system.pass_through_function(TABLE(t1) arg_alias)) table_alias");

        assertFails(MISSING_ATTRIBUTE,
                "line 1:23: 't1.a' cannot be resolved",
                "SELECT table_alias.x, t1.a FROM TABLE(system.pass_through_function(TABLE(t1) arg_alias)) table_alias");

        assertFails(MISSING_ATTRIBUTE,
                "line 1:23: 'table_alias.a' cannot be resolved",
                "SELECT table_alias.x, table_alias.a FROM TABLE(system.pass_through_function(TABLE(t1))) table_alias");
    }

    @Test
    public void testTableFunctionRequiredColumns()
    {
        // the function required_column_function specifies columns 0 and 1 from table argument "INPUT" as required.
        analyze("SELECT * FROM TABLE(system.required_columns_function(input => TABLE(t1)))");

        analyze("SELECT * FROM TABLE(system.required_columns_function(input => TABLE(SELECT 1, 2, 3)))");

        assertFails(TABLE_FUNCTION_IMPLEMENTATION_ERROR,
                "Invalid index: 1 of required column from table argument INPUT",
                "SELECT * FROM TABLE(system.required_columns_function(input => TABLE(SELECT 1)))");

        // table s1.t5 has two columns. The second column is hidden. Table function cannot require a hidden column.
        assertFails(TABLE_FUNCTION_IMPLEMENTATION_ERROR,
                "Invalid index: 1 of required column from table argument INPUT",
                "SELECT * FROM TABLE(system.required_columns_function(input => TABLE(s1.t5)))");
    }

    @Test
    public void testInvalidMerge()
    {
        assertFails(MISSING_TABLE, "Table tpch.s1.foo does not exist",
                "MERGE INTO foo USING bar ON foo.id = bar.id WHEN MATCHED THEN UPDATE SET id = bar.id + 1");

        assertFails(NOT_SUPPORTED, "line 1:1: Merging into views is not supported",
                "MERGE INTO v1 USING t1 ON v1.a = t1.a WHEN MATCHED THEN UPDATE SET id = bar.id + 1");

        assertFails(NOT_SUPPORTED, "line 1:1: Merging into materialized views is not supported",
                "MERGE INTO mv1 USING t1 ON mv1.a = t1.a WHEN MATCHED THEN  UPDATE SET id = bar.id + 1");
    }
}
