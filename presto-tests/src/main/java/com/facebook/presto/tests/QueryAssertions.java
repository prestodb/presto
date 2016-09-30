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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.tpch.TpchTable;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.OptionalLong;

import static com.facebook.presto.util.Types.checkType;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public final class QueryAssertions
{
    private static final Logger log = Logger.get(QueryAssertions.class);

    private QueryAssertions()
    {
    }

    public static void assertUpdate(QueryRunner queryRunner, Session session, @Language("SQL") String sql, OptionalLong count)
    {
        long start = System.nanoTime();
        MaterializedResult results = queryRunner.execute(session, sql);
        log.info("FINISHED in presto: %s", nanosSince(start));

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

    public static void assertQuery(QueryRunner actualQueryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering,
            boolean compareUpdate)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = null;
        try {
            actualResults = actualQueryRunner.execute(session, actual).toJdbcTypes();
        }
        catch (RuntimeException ex) {
            fail("Execution of 'actual' query failed: " + actual, ex);
        }
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = null;
        try {
            expectedResults = h2QueryRunner.execute(session, expected, actualResults.getTypes());
        }
        catch (RuntimeException ex) {
            fail("Execution of 'expected' query failed: " + actual, ex);
        }
        log.info("FINISHED in presto: %s, h2: %s, total: %s", actualTime, nanosSince(expectedStart), nanosSince(start));

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
            fail(format("%snot equal\nActual %s rows:\n    %s\nExpected %s rows:\n    %s\n",
                    message == null ? "" : (message + "\n"),
                    actualSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(actualSet, 100)),
                    expectedSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(expectedSet, 100))));
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

    public static void assertApproximateQuery(
            QueryRunner queryRunner,
            Session session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = queryRunner.execute(session, actual);
        log.info("FINISHED in %s", nanosSince(start));

        MaterializedResult expectedResults = h2QueryRunner.execute(session, expected, actualResults.getTypes());
        assertApproximatelyEqual(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
    }

    public static void assertApproximatelyEqual(List<MaterializedRow> actual, List<MaterializedRow> expected)
            throws Exception
    {
        // TODO: support GROUP BY queries
        assertEquals(actual.size(), 1, "approximate query returned more than one row");

        MaterializedRow actualRow = actual.get(0);
        MaterializedRow expectedRow = expected.get(0);

        for (int i = 0; i < actualRow.getFieldCount(); i++) {
            String actualField = (String) actualRow.getField(i);
            double actualValue = Double.parseDouble(actualField.split(" ")[0]);
            double error = Double.parseDouble(actualField.split(" ")[2]);
            Object expectedField = expectedRow.getField(i);
            assertTrue(expectedField instanceof String || expectedField instanceof Number);
            double expectedValue;
            if (expectedField instanceof String) {
                expectedValue = Double.parseDouble((String) expectedField);
            }
            else {
                expectedValue = ((Number) expectedField).doubleValue();
            }
            assertTrue(Math.abs(actualValue - expectedValue) < error);
        }
    }

    public static void copyTpchTables(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
            throws Exception
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTable(queryRunner, sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    public static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, Session session)
            throws Exception
    {
        QualifiedObjectName table = new QualifiedObjectName(sourceCatalog, sourceSchema, sourceTable);
        copyTable(queryRunner, table, session);
    }

    public static void copyTable(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getObjectName());
        @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getObjectName(), table);
        long rows = checkType(queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0), Long.class, "rows");
        log.info("Imported %s rows for %s in %s", rows, table.getObjectName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }
}
