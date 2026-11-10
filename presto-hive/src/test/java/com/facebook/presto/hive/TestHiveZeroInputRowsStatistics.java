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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveSessionProperties.COLLECT_COLUMN_STATISTICS_ON_WRITE;
import static org.testng.Assert.assertEquals;

public class TestHiveZeroInputRowsStatistics
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.of(),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.<String, String>builder()
                        .put("hive.quick-stats.enabled", "true")
                        .build(),
                Optional.empty());
    }

    // Without column statistics written at INSERT time, statistics come from quick stats
    private Session quickStatsSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().get(), COLLECT_COLUMN_STATISTICS_ON_WRITE, "false")
                .build();
    }

    /**
     * Quick stats are cached, so a table can still be reported as empty after it is written. A stale
     * zero may only affect the plan, never the results.
     */
    @Test
    public void testResultsUnchangedUnderStaleZeroRowCount()
    {
        Session session = quickStatsSession();
        assertUpdate(session, "CREATE TABLE stale_cached_empty (id BIGINT, name VARCHAR) WITH (format = 'PARQUET')");
        try {
            assertQuery(session, "SELECT count(*) FROM stale_cached_empty", "SELECT 0");
            assertEquals(reportedRowCount(session, "stale_cached_empty"), Double.valueOf(0.0), "expected a zero row count for an empty table");

            assertUpdate(session, "INSERT INTO stale_cached_empty VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

            assertEquals(reportedRowCount(session, "stale_cached_empty"), Double.valueOf(0.0), "expected the cached zero to still be served");

            assertQuery(session, "SELECT id, name FROM stale_cached_empty ORDER BY id", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery(session, "SELECT count(*) FROM stale_cached_empty", "SELECT 3");
            assertQuery(session, "SELECT sum(id) FROM stale_cached_empty", "SELECT 6");
            assertQuery(
                    session,
                    "SELECT t.id, t.name FROM stale_cached_empty t JOIN (VALUES 1, 2, 3) v(id) ON t.id = v.id ORDER BY t.id",
                    "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS stale_cached_empty");
        }
    }

    /**
     * An empty Parquet table reports a row count of 0 rather than unknown, through a real query runner.
     */
    @Test
    public void testEmptyTableReportsZeroRowCountEndToEnd()
    {
        assertUpdate("CREATE TABLE zero_rows_table (id BIGINT) WITH (format = 'PARQUET')");
        try {
            Double rowCount = reportedRowCount(getSession(), "zero_rows_table");
            assertEquals(rowCount, Double.valueOf(0.0), "An empty Parquet table must report rowCount 0, not UNKNOWN");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS zero_rows_table");
        }
    }

    /**
     * The {@code row_count} from {@code SHOW STATS}'s summary row (the one with a null column name).
     * {@code null} means UNKNOWN.
     */
    private Double reportedRowCount(Session session, String tableName)
    {
        MaterializedResult result = computeActual(session, "SHOW STATS FOR " + tableName);
        int columnNameIndex = 0;
        int rowCountIndex = 4;
        MaterializedRow summary = result.getMaterializedRows().stream()
                .filter(row -> row.getField(columnNameIndex) == null)
                .findFirst()
                .orElseThrow(() -> new AssertionError("SHOW STATS produced no summary row for " + tableName));
        return (Double) summary.getField(rowCountIndex);
    }
}
