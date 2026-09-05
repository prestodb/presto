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
import static com.facebook.presto.hive.HiveSessionProperties.QUICK_STATS_PROVABLE_EMPTY_ENABLED;
import static org.testng.Assert.assertEquals;

/**
 * End-to-end tests for the provable-zero change against a real Hive query runner.
 * <p>
 * this change is the property the whole design rests on: <b>a stale or wrong statistic may only degrade the
 * plan, never the answer.</b> This change makes that property load-bearing, because
 * {@code QuickStatsProvider.partitionToStatsCache} keeps a computed statistic for
 * {@code hive.quick-stats.cache-expiry} (24 h by default) and caches it unconditionally, so a table
 * that was provably empty at plan time and is written a moment later really will be described as
 * having zero rows for up to a day.
 * <p>
 * Column statistics collection on write is disabled here so that quick stats -- not the metastore's
 * own column statistics -- is the statistics source, which is what puts the cached zero on the path
 * {@code MetastoreHiveStatisticsProvider} actually consults.
 */
public class TestHiveProvablyEmptyStatistics
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
                        .put("hive.quick-stats.provable-empty-enabled", "true")
                        .build(),
                Optional.empty());
    }

    /**
     * Column statistics collection on write is disabled per session (the config property is hard-coded
     * to true by {@code HiveQueryRunner}), so that quick stats -- not metastore column statistics
     * written at INSERT time -- is the statistics source. That is what puts the cached zero on the path
     * {@code MetastoreHiveStatisticsProvider} actually consults.
     */
    private Session quickStatsSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(getSession().getCatalog().get(), COLLECT_COLUMN_STATISTICS_ON_WRITE, "false")
                .build();
    }

    /**
     * A table whose statistics say zero rows but which actually contains rows must still return
     * every row.
     * <p>
     * The stale zero is produced by the real provable-zero path, not injected: the table is created empty, a
     * statistics-consuming query makes {@code ParquetQuickStatsBuilder} prove emptiness from the (empty)
     * file listing, and {@code QuickStatsProvider} caches that zero. Rows are then inserted. The cached
     * statistic is now wrong -- and the query results must be completely unaffected by it.
     */
    @Test
    public void testResultsUnchangedUnderStaleEmptiness()
    {
        Session session = quickStatsSession();
        assertUpdate(session, "CREATE TABLE stale_cached_empty (id BIGINT, name VARCHAR) WITH (format = 'PARQUET')");
        try {
            // Populate the quick stats cache while the table genuinely has no files.
            assertQuery(session, "SELECT count(*) FROM stale_cached_empty", "SELECT 0");
            assertEquals(reportedRowCount(session, "stale_cached_empty"), Double.valueOf(0.0), "expected a provable zero for an empty table");

            assertUpdate(session, "INSERT INTO stale_cached_empty VALUES (1, 'a'), (2, 'b'), (3, 'c')", 3);

            // The statistic is stale by construction now; assert that before relying on it, so this test
            // cannot silently degrade into asserting nothing.
            assertEquals(reportedRowCount(session, "stale_cached_empty"), Double.valueOf(0.0), "expected the cached zero to still be served");

            // Whatever the statistics say, every row must still come back.
            assertQuery(session, "SELECT id, name FROM stale_cached_empty ORDER BY id", "VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            assertQuery(session, "SELECT count(*) FROM stale_cached_empty", "SELECT 3");
            assertQuery(session, "SELECT sum(id) FROM stale_cached_empty", "SELECT 6");
            // A join with the stale-empty table on the probe side -- the shape this change deliberately flips --
            // must also return every matching row.
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
     * An actually-empty Parquet table reports a row count of <b>0</b> rather
     * than UNKNOWN. This is the observable output of the whole provable-zero chain -- proof in
     * {@code ParquetQuickStatsBuilder}, conversion in {@code PartitionQuickStats}, aggregation in
     * {@code MetastoreHiveStatisticsProvider} -- through a real query runner rather than at the seams.
     */
    @Test
    public void testEmptyTableReportsZeroRowCountEndToEnd()
    {
        assertUpdate("CREATE TABLE provably_empty_table (id BIGINT) WITH (format = 'PARQUET')");
        try {
            Double rowCount = reportedRowCount(getSession(), "provably_empty_table");
            assertEquals(rowCount, Double.valueOf(0.0), "An empty Parquet table must report rowCount 0, not UNKNOWN");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS provably_empty_table");
        }
    }

    /**
     * With the kill-switch off, the same empty table reports UNKNOWN again. Set as a
     * session property so it exercises the session override alongside the config default.
     */
    @Test
    public void testKillSwitchRestoresUnknownEndToEnd()
    {
        assertUpdate("CREATE TABLE kill_switch_empty_table (id BIGINT) WITH (format = 'PARQUET')");
        try {
            Double rowCount = reportedRowCount(
                    Session.builder(getSession())
                            .setCatalogSessionProperty(getSession().getCatalog().get(), QUICK_STATS_PROVABLE_EMPTY_ENABLED, "false")
                            .build(),
                    "kill_switch_empty_table");
            assertEquals(rowCount, null, "Kill-switch off must restore the pre-change UNKNOWN row count");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS kill_switch_empty_table");
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
