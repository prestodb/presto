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
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQueryRunner;
import static com.facebook.presto.spi.StandardWarningCode.MULTIPLE_ORDER_BY;
import static com.facebook.presto.spi.StandardWarningCode.PARSER_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.PERFORMANCE_WARNING;
import static com.facebook.presto.spi.StandardWarningCode.TOO_MANY_STAGES;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.difference;
import static org.testng.Assert.assertTrue;

public class TestWarnings
{
    private static final int STAGE_COUNT_WARNING_THRESHOLD = 20;
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
