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
import com.facebook.airlift.units.Duration;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.transaction.TransactionId;
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
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import static com.facebook.airlift.units.Duration.nanosSince;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class QueryAssertions
{
    private static final Logger log = Logger.get(QueryAssertions.class);

    private QueryAssertions()
    {
    }

    public static Session assertStartTransaction(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        MaterializedResult results = queryRunner.execute(session, sql);
        if (!results.getUpdateInfo().isPresent()) {
            fail("update type is not set");
        }
        if (!results.getUpdateInfo().get().getUpdateType().equals("START TRANSACTION")) {
            fail("not a start transaction statement");
        }
        assertTrue(results.getStartedTransactionId().isPresent());
        assertFalse(results.isClearTransactionId());

        TransactionId transactionId = results.getStartedTransactionId().get();
        return session.beginTransactionId(transactionId, queryRunner.getTransactionManager(), queryRunner.getAccessControl());
    }

    public static Session assertEndTransaction(QueryRunner queryRunner, Session session, @Language("SQL") String sql)
    {
        MaterializedResult results = queryRunner.execute(session, sql);
        if (!results.getUpdateInfo().isPresent()) {
            fail("update type is not set");
        }
        if (!results.getUpdateInfo().get().getUpdateType().equals("ROLLBACK") && !results.getUpdateInfo().get().getUpdateType().equals("COMMIT")) {
            fail("not a end transaction statement");
        }
        assertTrue(results.isClearTransactionId());
        assertFalse(results.getStartedTransactionId().isPresent());

        return session.clearTransaction(queryRunner.getTransactionManager(), queryRunner.getAccessControl());
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

        if (!results.getUpdateInfo().isPresent()) {
            fail("update type is not set");
        }

        if (results.getUpdateCount().isPresent()) {
            if (!count.isPresent()) {
                fail("update count must be specified. Expected updated count : " + results.getUpdateCount().getAsLong());
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
        assertQuery(actualQueryRunner, session, actual, expectedQueryRunner, session, expected, ensureOrdering, compareUpdate, Optional.empty());
    }

    public static void assertQuery(
            QueryRunner actualQueryRunner,
            Session actualSession,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            Session expectedSession,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate)
    {
        assertQuery(actualQueryRunner, actualSession, actual, expectedQueryRunner, expectedSession, expected, ensureOrdering, compareUpdate, Optional.empty());
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
        assertQuery(actualQueryRunner, session, actual, expectedQueryRunner, session, expected, ensureOrdering, compareUpdate, Optional.of(planAssertion));
    }

    public static void assertQuery(
            QueryRunner actualQueryRunner,
            Session actualSession,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            Session expectedSession,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate,
            Consumer<Plan> planAssertion)
    {
        assertQuery(actualQueryRunner, actualSession, actual, expectedQueryRunner, expectedSession, expected, ensureOrdering, compareUpdate, Optional.of(planAssertion));
    }

    private static void assertQuery(
            QueryRunner actualQueryRunner,
            Session actualSession,
            @Language("SQL") String actual,
            ExpectedQueryRunner expectedQueryRunner,
            Session expectedSession,
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
                MaterializedResultWithPlan resultWithPlan = actualQueryRunner.executeWithPlan(actualSession, actual, WarningCollector.NOOP);
                queryPlan = resultWithPlan.getQueryPlan();
                actualResults = resultWithPlan.getMaterializedResult().toTestTypes();
            }
            catch (RuntimeException ex) {
                fail("Execution of 'actual' query failed: " + actual, ex);
            }
        }
        else {
            try {
                actualResults = actualQueryRunner.execute(actualSession, actual).toTestTypes();
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
            expectedResults = expectedQueryRunner.execute(expectedSession, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + expected, ex);
        }
        Duration totalTime = nanosSince(start);
        if (totalTime.compareTo(Duration.succinctDuration(1, SECONDS)) > 0) {
            log.info("FINISHED in presto: %s, expected: %s, total: %s", actualTime, nanosSince(expectedStart), totalTime);
        }

        if (actualResults.getUpdateInfo().isPresent() || actualResults.getUpdateCount().isPresent()) {
            if (!actualResults.getUpdateInfo().isPresent()) {
                fail("update count present without update type for query: \n" + actual);
            }
            if (!compareUpdate) {
                fail("update type should not be present (use assertUpdate) for query: \n" + actual);
            }
        }

        List<MaterializedRow> actualRows = actualResults.getMaterializedRows();
        List<MaterializedRow> expectedRows = expectedResults.getMaterializedRows();

        if (compareUpdate) {
            if (!actualResults.getUpdateInfo().isPresent()) {
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
                assertEquals(actualRows,
                        expectedRows,
                        "For query: \n " + actual + "\n actual column types:\n " + actualResults.getTypes() + "\nexpected column types:\n" + expectedResults.getTypes() + "\n");
            }
        }
        else {
            assertEqualsIgnoreOrder(actualRows,
                    expectedRows,
                    "For query: \n " + actual + "\n actual column types:\n " + actualResults.getTypes() + "\nexpected column types:\n" + expectedResults.getTypes() + "\n");
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
            String extraRowsMessage = "";
            if (!unexpectedRows.isEmpty()) {
                int numShown = Math.min(limit, unexpectedRows.size());
                extraRowsMessage = format(
                        "Actual rows (%s of %s extra rows shown, %s rows in total):\n    %s\n",
                        numShown,
                        unexpectedRows.size(),
                        actualSet.size(),
                        Joiner.on("\n    ").join(Iterables.limit(unexpectedRows, limit)));
            }
            String missingRowsMessage = "";
            if (!missingRows.isEmpty()) {
                int numShown = Math.min(limit, missingRows.size());
                missingRowsMessage = format(
                        "Expected rows (%s of %s missing rows shown, %s rows in total):\n    %s\n",
                        numShown,
                        missingRows.size(),
                        expectedSet.size(),
                        Joiner.on("\n    ").join(Iterables.limit(missingRows, limit)));
            }
            String rowsDiff = format(
                    "%snot equal\n%s%s",
                    message == null ? "" : (message + "\n"),
                    extraRowsMessage,
                    missingRowsMessage);
            fail(rowsDiff);
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
                fail(format("expected row missing: %s%nAll %s rows:%n    %s%nExpected subset %s rows:%n    %s%n",
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
            assertExceptionMessage(sql, ex, expectedMessageRegExp, false, false);
        }
    }

    protected static void assertQueryFails(QueryRunner queryRunner, Session session, @Language("SQL") String sql, @Language("RegExp") String expectedMessageRegExp, boolean usePatternMatcher, boolean exact)
    {
        try {
            queryRunner.execute(session, sql);
            fail(format("Expected query to fail: %s", sql));
        }
        catch (RuntimeException ex) {
            assertExceptionMessage(sql, ex, expectedMessageRegExp, usePatternMatcher, exact);
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

    public static void assertExceptionMessage(String sql, Exception exception, @Language("RegExp") String regex, boolean usePatternMatcher, boolean exact)
    {
        if (usePatternMatcher) {
            Pattern p = Pattern.compile(regex, Pattern.MULTILINE);
            if (!(p.matcher(exception.getMessage()).find())) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
            }
        }
        else {
            if (!(exact ? nullToEmpty(exception.getMessage()).equals(regex) : nullToEmpty(exception.getMessage()).matches(regex))) {
                fail(format("Expected exception message '%s' to match '%s' for query: %s", exception.getMessage(), regex, sql), exception);
            }
        }
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        copyTables(
                queryRunner,
                sourceCatalog,
                sourceSchema,
                session,
                Iterables.transform(tables, table -> table.getTableName()),
                false,
                false);
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables,
            boolean ifNotExists)
    {
        copyTables(
                queryRunner,
                sourceCatalog,
                sourceSchema,
                session,
                Iterables.transform(tables, table -> table.getTableName()),
                ifNotExists,
                false);
    }

    public static void copyTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<String> tables,
            boolean ifNotExists,
            boolean bucketed)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (String table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, session, table, ifNotExists, bucketed);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    public static void copyTable(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            String sourceTable,
            boolean ifNotExists,
            boolean bucketed)
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);

        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());

        @Language("SQL") String sql = getCopyTableSql(sourceCatalog, table, ifNotExists, bucketed);
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);

        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private static String getCopyTableSql(String sourceCatalog, QualifiedObjectName table, boolean ifNotExists, boolean bucketed)
    {
        if (!bucketed) {
            return format("CREATE TABLE %s %s AS SELECT * FROM %s", ifNotExists ? "IF NOT EXISTS" : "", table.getObjectName(), table);
        }
        else {
            switch (sourceCatalog) {
                case "tpch":
                    return getTpchCopyTableSqlBucketed(table);
                case "tpcds":
                    return getTpcdsCopyTableSqlBucketed(table);
                default:
                    throw new UnsupportedOperationException();
            }
        }
    }

    private static String getTpchCopyTableSqlBucketed(QualifiedObjectName table)
    {
        @Language("SQL") String sql;
        switch (table.getObjectName()) {
            case "part":
            case "partsupp":
            case "supplier":
            case "nation":
            case "region":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "lineitem":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['orderkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "customer":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['custkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "orders":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['custkey'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return sql;
    }

    private static String getTpcdsCopyTableSqlBucketed(QualifiedObjectName table)
    {
        @Language("SQL") String sql;
        switch (table.getObjectName()) {
            case "call_center":
            case "catalog_page":
            case "customer":
            case "customer_address":
            case "customer_demographics":
            case "date_dim":
            case "household_demographics":
            case "income_band":
            case "item":
            case "promotion":
            case "reason":
            case "ship_mode":
            case "store":
            case "time_dim":
            case "warehouse":
            case "web_page":
            case "web_site":
                sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "inventory":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['inv_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "store_returns":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['sr_returned_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "store_sales":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['ss_sold_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "web_returns":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['wr_returned_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "web_sales":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['ws_sold_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "catalog_returns":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['cr_returned_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            case "catalog_sales":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['cs_sold_date_sk'], bucket_count=11) AS SELECT * FROM %s", table.getObjectName(), table);
                break;
            default:
                throw new UnsupportedOperationException();
        }

        return sql;
    }
}
