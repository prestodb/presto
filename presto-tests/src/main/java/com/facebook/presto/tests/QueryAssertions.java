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

import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMultiset;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.util.List;

import static com.facebook.presto.util.Types.checkType;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
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

    public static void assertQuery(QueryRunner actualQueryRunner,
            ConnectorSession actualSession,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected,
            boolean ensureOrdering)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = actualQueryRunner.execute(actualSession, actual).toJdbcTypes();
        Duration actualTime = nanosSince(start);

        long expectedStart = System.nanoTime();
        MaterializedResult expectedResults = h2QueryRunner.execute(expected, actualResults.getTypes());
        log.info("FINISHED in presto: %s, h2: %s, total: %s", actualTime, nanosSince(expectedStart), nanosSince(start));

        if (ensureOrdering) {
            assertEquals(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
        }
        else {
            assertEqualsIgnoreOrder(actualResults.getMaterializedRows(), expectedResults.getMaterializedRows());
        }
    }

    public static void assertEqualsIgnoreOrder(Iterable<?> actual, Iterable<?> expected)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expected, "expected is null");

        ImmutableMultiset<?> actualSet = ImmutableMultiset.copyOf(actual);
        ImmutableMultiset<?> expectedSet = ImmutableMultiset.copyOf(expected);
        if (!actualSet.equals(expectedSet)) {
            fail(format("not equal\nActual %s rows:\n    %s\nExpected %s rows:\n    %s\n",
                    actualSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(actualSet, 100)),
                    expectedSet.size(),
                    Joiner.on("\n    ").join(Iterables.limit(expectedSet, 100))));
        }
    }

    public static void assertApproximateQuery(
            QueryRunner queryRunner,
            ConnectorSession session,
            @Language("SQL") String actual,
            H2QueryRunner h2QueryRunner,
            @Language("SQL") String expected)
            throws Exception
    {
        long start = System.nanoTime();
        MaterializedResult actualResults = queryRunner.execute(session, actual);
        log.info("FINISHED in %s", nanosSince(start));

        MaterializedResult expectedResults = h2QueryRunner.execute(expected, actualResults.getTypes());
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

    public static void copyAllTables(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, ConnectorSession session)
            throws Exception
    {
        for (QualifiedTableName table : queryRunner.listTables(session, sourceCatalog, sourceSchema)) {
            if (table.getTableName().equalsIgnoreCase("dual")) {
                continue;
            }
            copyTable(queryRunner, table, session);
        }
    }

    public static void copyTable(QueryRunner queryRunner, String sourceCatalog, String sourceSchema, String sourceTable, ConnectorSession session)
            throws Exception
    {
        QualifiedTableName table = new QualifiedTableName(sourceCatalog, sourceSchema, sourceTable);
        copyTable(queryRunner, table, session);
    }

    public static void copyTable(QueryRunner queryRunner, QualifiedTableName table, ConnectorSession session)
    {
        long start = System.nanoTime();
        log.info("Running import for %s", table.getTableName());
        @Language("SQL") String sql = format("CREATE TABLE %s AS SELECT * FROM %s", table.getTableName(), table);
        long rows = checkType(queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0), Long.class, "rows");
        log.info("Imported %s rows for %s in %s", rows, table.getTableName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }
}
