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
package com.facebook.presto.tests;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.QueryRunner.MaterializedResultWithPlan;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import io.airlift.tpch.TpchTable;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public final class QueryAssertions
{
    private static final Logger log = Logger.get(QueryAssertions.class);

    private QueryAssertions()
    {
    }

    public static void assertUpdate(QueryRunner queryRunner, Session session, @Language("SQL") String sql, OptionalLong count, Optional<Consumer<Plan>> planAssertion)
    {
        long start = System.nanoTime();
        MaterializedResult results;
        Plan queryPlan;
        if (planAssertion.isPresent()) {
            MaterializedResultWithPlan resultWithPlan = queryRunner.executeWithPlan(session, sql, WarningCollector.NOOP);
            queryPlan = resultWithPlan.getQueryPlan();
            results = resultWithPlan.getMaterializedResult().toTestTypes();
        }
        else {
            queryPlan = null;
            results = queryRunner.execute(session, sql);
        }
        Duration queryTime = nanosSince(start);
        if (queryTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.info("FINISHED in presto: %s", queryTime);
        }

        if (planAssertion.isPresent()) {
            planAssertion.get().accept(queryPlan);
        }

        if (!results.getUpdateType().isPresent()) {
            fail("update type is not set");
        }

        if (results.getUpdateCount().isPresent()) {
            if (!count.isPresent()) {
                fail("update count should not be present");
            }
            assertEquals(results.getUpdateCount().getAsLong(), count.getAsLong(), "update count");
        }
        else if (count.isPresent()) {
            fail("update count is not present");
        }
    }

    public static void assertQuery(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate)
    {
        assertQuery(actualQueryRunner, session, actual, expectedQueryRunner, expected, ensureOrdering, compareUpdate, Optional.empty());
    }

    public static void assertQuery(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Consumer<Plan> planAssertion)
    {
        assertQuery(actualQueryRunner, session, actual, expectedQueryRunner, expected, ensureOrdering, compareUpdate, Optional.of(planAssertion));
    }

    private static void assertQuery(
            QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Optional<Consumer<Plan>> planAssertion)
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = null;
        Plan queryPlan = null;
        if (planAssertion.isPresent()) {
            try {
                MaterializedResultWithPlan resultWithPlan = actualQueryRunner.executeWithPlan(session, actual, WarningCollector.NOOP);
                queryPlan = resultWithPlan.getQueryPlan();
                actualResults = resultWithPlan.getMaterializedResult().toTestTypes();
            }
            catch (RuntimeException ex) {
                fail("Execution of 'actual' query failed: " + actual, ex);
            }
        }
        else {
            try {
                actualResults = actualQueryRunner.execute(session, actual).toTestTypes();
            }
            catch (RuntimeException ex) {
                fail("Execution of 'actual' query failed: " + actual, ex);
            }
        }
        if (planAssertion.isPresent()) {
            planAssertion.get().accept(queryPlan);
        }
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = null;
        try {
            expectedResults = expectedQueryRunner.execute(session, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }
        Duration totalTime = nanosSince(start);
        if (totalTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.info("FINISHED in presto: %s, expected: %s, total: %s", actualTime, nanosSince(expectedStart), totalTime);
        }

        if (actualResults.getUpdateType().isPresent() || actualResults.getUpdateCount().isPresent()) {
            if (!actualResults.getUpdateType().isPresent()) {
                fail("update count present without update type for query: \n" + actual);
            }
            if (!compareUpdate) {
                fail("update type should not be present (use assertUpdate) for query: \n" + actual);
            }
        }

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (compareUpdate) {
            if (!actualResults.getUpdateType().isPresent()) {
                fail("update type not present for query: \n" + actual);
            }
            if (!actualResults.getUpdateCount().isPresent()) {
                fail("update count not present for query: \n" + actual);
            }
            assertEquals(actualRows.size(), 1, "For query: \n " + actual + "\n:");
            assertEquals(expectedRows.size(), 1, "For query: \n " + actual + "\n:");
            MaterializedRow row = expectedRows.get(0);
            assertEquals(row.getFieldCount(), 1, "For query: \n " + actual + "\n:");
            assertEquals(row.getField(0), actualResults.getUpdateCount().getAsLong(), "For query: \n " + actual + "\n:");
        }

        if (ensureOrdering) {
            if (!actualRows.equals(expectedRows)) {
                assertEquals(actualRows, expectedRows, "For query: \n " + actual + "\n:");
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows, expectedRows, "For query: \n " + actual);
        }
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertEqualsIgnoreOrder(actual, expected, null);
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected, String message)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expected, "expected is null");

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expected);
        if (!actualSet.equals(expectedSet)) {
            Multiset<?> unexpectedRows = Multisets.difference(actualSet, expectedSet);
            Multiset<?> missingRows = Multisets.difference(expectedSet, actualSet);
            int limit = 100;
            fail(format(
                    "%snot equal\n" +
                            "Actual rows (up to %s of %s extra rows shown, %s rows in total):\n    %s\n" +
                            "Expected rows (up to %s of %s missing rows shown, %s rows in total):\n    %s\n",
                    message == null ? "" : (message + "\n"),
                    limit,
                    unexpectedRows.size(),
                    actualSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(unexpectedRows, limit)),
                    limit,
                    missingRows.size(),
                    expectedSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(missingRows, limit))));
        }
    }

    public static void assertContainsEventually(Supplier<MaterializedResult> all, MaterializedResult expectedSubset, Duration timeout)
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                assertContains(all.get(), expectedSubset);
                return;
            }
            catch (AssertionError e) {
                if (nanosSince(start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            sleepUninterruptibly(50, MILLISECONDS);
        }
    }

    public static void assertContains(MaterializedResult all, MaterializedResult expectedSubset)
    {
        for (MaterializedRow row : expectedSubset.getMaterializedRows()) {
            if (!all.getMaterializedRows().contains(row)) {
                fail(format("expected row missing: %s\nAll %s rows:\n    %s\nExpected subset %s rows:\n    %s\n",
                        row,
                        all.getMaterializedRows().size(),
                        Joiner.on("\n    ").join(Iterables.limit(all, 100)),
                        expectedSubset.getMaterializedRows().size(),
                        Joiner.on("\n    ").join(Iterables.limit(expectedSubset, 100))));
            }
        }
    }

    protected static void assertQuerySucceeds(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        try {
            queryRunner.execute(session, sql);
        }
        catch (RuntimeException e) {
            fail(format("Expected query to succeed: %s", sql), e);
        }
    }

    protected static void assertQueryFailsEventually(QueryRunner queryRunner, Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, Duration timeout)
    {
        long start = System.nanoTime();
        while (!Thread.currentThread().isInterrupted()) {
            try {
                assertQueryFails(queryRunner, session, sql, expectedMessageRegExp);
                return;
            }
            catch (AssertionError e) {
                if (nanosSince(start).compareTo(timeout) > 0) {
                    throw e;
                }
            }
            sleepUninterruptibly(50, MILLISECONDS);
        }
    }

    protected static void assertQueryFails(QueryRunner queryRunner, Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp)
    {
        try {
            queryRunner.execute(session, sql);
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException ex) {
            assertExceptionMessage(sql, ex, expectedMessageRegExp);
        }
    }

    protected static void assertQueryReturnsEmptyResult(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        try {
            MaterializedResult results = queryRunner.execute(session, sql).toTestTypes();
            assertNotNull(results);
            assertEquals(results.getRowCount(), 0);
        }
        catch (RuntimeException ex) {
            fail("Execution of query failed: " + sql, ex);
        }
    }

    private static void assertExceptionMessage(String sql, Exception exception, @Language("RegExp") String regex)
    {
        if (!nullToEmpty(exception.getMessage()).matches(regex)) {
            fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
        }
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        copyTpchTables(queryRunner, sourceCatalog, sourceSchema, session, tables, false);
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables,
            boolean ifNotExists)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session, ifNotExists);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session, boolean ifNotExists)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        copyTable(queryRunner, table, session, ifNotExists);
    }

    private static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session, boolean ifNotExists)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        @Language("SQL") String sql = format("CREATE TABLE %s %s AS SELECT * FROM %s", ifNotExists ? "IF NOT EXISTS" : "", table.getObjectName(), table);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }
}
