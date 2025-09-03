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
package com.facebook.presto.execution;

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Set;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.WARN_ON_COMMON_NAN_PATTERNS;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;
import static com.facebook.presto.spi.StandardWarningCode.MULTIPLE_ORDER_BY;
import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.SEMANTIC_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.TOO_MANY_STAGES;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static org.testng.Assert.assertTrue;

public class TestWarnings
{
    private static final int STAGE_COUNT_WARNING_THRESHOLD = 20;
    private static final Session ALL_WARININGS_SESSION = Session.builder(TEST_SESSION)
            .setSystemProperty(WARN_ON_COMMON_NAN_PATTERNS, "true")
            .build();
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = createQueryRunner(ImmutableMap.of("query.stage-count-warning-threshold", String.valueOf(STAGE_COUNT_WARNING_THRESHOLD)));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test
    public void testStageCountWarningThreshold()
    {
        StringBuilder queryBuilder = new StringBuilder("SELECT name FROM nation WHERE nationkey = 0");
        String noWarningsQuery = queryBuilder.toString();
        for (int stageIndex = 1; stageIndex <= STAGE_COUNT_WARNING_THRESHOLD; stageIndex++) {
            queryBuilder.append("  UNION")
                    .append("  SELECT name FROM nation WHERE nationkey = ")
                    .append(stageIndex);
        }
        String query = queryBuilder.toString();
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(TOO_MANY_STAGES.toWarningCode(), PERFORMANCE_WARNING.toWarningCode()));
        assertWarnings(queryRunner, TEST_SESSION, noWarningsQuery, ImmutableSet.of());
    }

    @Test
    public void testNonReservedWordWarning()
    {
        String query = "SELECT CURRENT_ROLE, t.current_role FROM (VALUES (3)) t(current_role)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));
    }

    @Test
    public void testNewReservedWordsWarning()
    {
        String query = "SELECT CALLED, t.called FROM (VALUES (3)) t(called)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));
    }

    @Test
    public void testWarningsAreNotStateful()
    {
        String query = "SELECT CALLED, t.called FROM (VALUES (3)) t(called)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));

        // Make sure the previous warning is not carried into the next query
        query = "SELECT UNCALLED, t.uncalled FROM (VALUES (3)) t(uncalled)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testQuotedIdentifiersDoNotTriggerWarning()
    {
        String query = "SELECT \"CALLED\" FROM (VALUES (3)) t(\"called\")";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testMixAndOrWarnings()
    {
        String query = "select true or false";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select true or false and false";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));

        query = "select true or (false and false)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select true or false or false";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select true and false and false";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select (true or false) and false";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select true and false or false and true";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));

        query = "select true or false and false or true";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));

        query = "select (true or false) and false or true";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));

        query = "select true or false and (false or true)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));

        query = "select (true or false) and (false or true)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select (true and true) or (true and false or false and true)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(PARSER_WARNING.toWarningCode()));
    }

    @Test
    public void testMultipleOrderByWarnings()
    {
        String query = "SELECT ARRAY_AGG( x ORDER BY x ASC, y DESC ) FROM ( SELECT 0 as x, 0 AS y)";
        assertWarnings(
                queryRunner, TEST_SESSION, query, ImmutableSet.of(MULTIPLE_ORDER_BY.toWarningCode()));
    }

    @Test
    public void testMapWithDoubleKeysProducesWarnings()
    {
        String query = "select map_from_entries(map_entries(MAP(ARRAY[12E2, 2.3, 3.4], ARRAY['x', 'y', 'z'])))";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select CAST(MAP(ARRAY[7E2,5.2,3.3,1.1], ARRAY[8,6,4,2]) AS JSON)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select CARDINALITY(MAP(ARRAY [12E-2], ARRAY [2.2]))";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select element_at(MAP(ARRAY [123e3], ARRAY [1e0]), 2)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select MAP(ARRAY [134E-2, 3.12], ARRAY [2.0E0, 4.0E0])[1.34]";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select MAP_CONCAT(MAP(ARRAY [11E1], ARRAY [2.2]), MAP(ARRAY [5.1], ARRAY [33.2]))";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select multimap_from_entries(ARRAY[(12E-2, 'x'), (2.3, 'y'), (1.2, 'a'), (3.4, 'b'), (2.3, 'c'), (3.4, null)])";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));

        query = "select transform_keys(map(ARRAY [25.5E0, 26.5E0, 27.5E0], ARRAY [25.5E0, 26.5E0, 27.5E0]), (k, v) -> k + v)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode(), PERFORMANCE_WARNING.toWarningCode()));

        query = "SELECT histogram(RETAILPRICE) FROM tpch.tiny.part";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));
    }

    @Test
    public void testMapWithNonDoubleKeyProducesNoWarnings()
    {
        String query = "select map_from_entries(map_entries(MAP(ARRAY[1, 2, 3], ARRAY['x', 'y', 'z'])))";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "SELECT histogram(TYPE) FROM tpch.tiny.part";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testMapWithDecimalKeyProducesNoWarnings()
    {
        String query = "select map_from_entries(map_entries(MAP(ARRAY[1.2, 2.3, 3.4], ARRAY['x', 'y', 'z'])))";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());

        query = "select CAST(MAP(ARRAY[7.2,5.2,3.3,1.1], ARRAY[8,6,4,2]) AS JSON)";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());
    }

    /**
     * The below tests check warnings for nan on DOUBLE/REAL types. Because we usually don't know whether any input values are nan or will produce nan,
     * the warnings only check that the type of the input can be affected by nans.
     */
    @Test
    public void testDoubleDivisionNanWarning()
    {
        String query = "SELECT x /y FROM (VALUES (DOUBLE '1.0', DOUBLE '2.0')) t(x, y)";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));
    }

    @Test
    public void testRealDivisionNanWarning()
    {
        String query = "SELECT x/y FROM (VALUES (REAL '1.0' , REAL '2.0')) t(x,y)";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));
    }

    @Test
    public void testConstantDivisionProducesNoWarnings()
    {
        String query = "SELECT DOUBLE '1.0' / DOUBLE '2.0'";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testIntegerDivisionProducesNoWarnings()
    {
        String query = "SELECT 4 / 2";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testNoWarningsForDivisionWhenDisabled()
    {
        String query = "SELECT DOUBLE '1.0' / DOUBLE '2.0'";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testOtherArithmeticOperationsProducesNoWarnings()
    {
        String query = "SELECT DOUBLE '1.0' * DOUBLE '2.0'";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testDoubleComparisonNaNWarning()
    {
        String query = "SELECT DOUBLE '1.0' > DOUBLE '2.0'";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));
    }

    @Test
    public void testRealComparisonNaNWarning()
    {
        String query = "SELECT REAL '1.0' > REAL '2.0'";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of(SEMANTIC_WARNING.toWarningCode()));
    }

    @Test
    public void testIntegerComparisonProducesNoWarnings()
    {
        String query = "SELECT 1 > 2";
        assertWarnings(queryRunner, ALL_WARININGS_SESSION, query, ImmutableSet.of());
    }

    @Test
    public void testNoWarningsForComparisonWhenDisabled()
    {
        String query = "SELECT DOUBLE '1.0' > DOUBLE '2.0'";
        assertWarnings(queryRunner, TEST_SESSION, query, ImmutableSet.of());
    }

    private static void assertWarnings(QueryRunner queryRunner, Session session, @Language("SQL") String sql, Set<WarningCode> expectedWarnings)
    {
        Set<WarningCode> warnings = queryRunner.execute(session, sql).getWarnings().stream()
                .map(PrestoWarning::getWarningCode)
                .collect(toImmutableSet());
        Set<WarningCode> expectedButMissing = difference(expectedWarnings, warnings);
        Set<WarningCode> unexpectedWarnings = difference(warnings, expectedWarnings);
        assertTrue(expectedButMissing.isEmpty(), "Expected warnings: " + expectedButMissing);
        assertTrue(unexpectedWarnings.isEmpty(), "Unexpected warnings: " + unexpectedWarnings);
    }
}
