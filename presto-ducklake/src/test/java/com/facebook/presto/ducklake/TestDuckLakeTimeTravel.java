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
package com.facebook.presto.ducklake;

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.stream.Collectors;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

/**
 * End-to-end time travel tests against the {@link TestingDuckLakeCatalog} fixture: {@code FOR
 * SYSTEM_VERSION}/{@code FOR SYSTEM_TIME} is a predicate that DuckLake threads through schema
 * lookup, column lookup, data file listing, inlined rows, positional deletes and compacted
 * ("partial") files alike, and each of those pieces already has its own focused test elsewhere
 * ({@link TestDuckLakeMetadataQueries}, {@link TestDuckLakeReads}). This class instead proves the
 * pieces compose: that a single version/timestamp resolution, applied consistently, makes table
 * existence, row counts and row contents all agree on the same point in the catalog's history.
 *
 * <p>One thing time travel does <b>not</b> pin is which column names are legal to write in SQL:
 * the engine analyzes every table reference's columns against its <em>current</em> (latest)
 * schema regardless of any {@code FOR SYSTEM_VERSION}/{@code FOR SYSTEM_TIME} clause -- this is a
 * property of the shared analyzer ({@code StatementAnalyzer.visitTable}), not a DuckLake choice,
 * and Presto's Iceberg connector is subject to the exact same behavior. So a query against an
 * older snapshot can only name columns that still exist (under their current name) at the latest
 * schema; it still gets that older snapshot's data (row set and values, including defaults for
 * columns that did not exist yet when a row was written), just resolved and named via the current
 * schema. Do not "fix" this in the connector: there is nothing to fix here, and per-version
 * column metadata would need to come from the engine's analysis phase, which today always uses
 * the unversioned lookup for column resolution.
 */
public class TestDuckLakeTimeTravel
        extends AbstractTestQueryFramework
{
    private static final DateTimeFormatter TIMESTAMP_FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneOffset.UTC);

    private DuckLakeQueryRunner duckLakeQueryRunner;

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        duckLakeQueryRunner = DuckLakeQueryRunner.createQueryRunner();
        return duckLakeQueryRunner.getQueryRunner();
    }

    @AfterClass(alwaysRun = true)
    @Override
    public void close()
            throws Exception
    {
        super.close();
        duckLakeQueryRunner.getTestingCatalog().close();
    }

    // ---- $snapshots lookup helpers: every id/time below is derived from the fixture's own
    // ---- catalog history rather than hardcoded, keyed off the stable "changes"/"commit_message"
    // ---- strings documented in the fixture loader.

    private List<Long> snapshotIdsByChanges(String changes)
    {
        return computeActual(format("SELECT snapshot_id FROM tpch.\"orders$snapshots\" WHERE changes = '%s' ORDER BY snapshot_id", changes))
                .getMaterializedRows().stream()
                .map(row -> (Long) row.getField(0))
                .collect(Collectors.toList());
    }

    private long snapshotIdByChanges(String changes)
    {
        return snapshotIdsByChanges(changes).get(0);
    }

    private long snapshotIdByCommitMessage(String commitMessage)
    {
        return (Long) computeActual(
                format("SELECT snapshot_id FROM tpch.\"orders$snapshots\" WHERE commit_message = '%s'", commitMessage))
                .getOnlyValue();
    }

    private Instant snapshotTime(long snapshotId)
    {
        Object value = computeActual(
                format("SELECT snapshot_time FROM tpch.\"orders$snapshots\" WHERE snapshot_id = %d", snapshotId))
                .getOnlyValue();
        return ((ZonedDateTime) value).toInstant();
    }

    /** A {@code TIMESTAMP ... UTC} literal for the given instant, truncated to millisecond precision. */
    private static String timestampLiteral(Instant instant)
    {
        return "TIMESTAMP '" + TIMESTAMP_FORMAT.format(instant.truncatedTo(ChronoUnit.MILLIS)) + " UTC'";
    }

    /** A literal one millisecond after {@code snapshotTime}, i.e. the earliest instant that still resolves "AS OF" to that snapshot. */
    private static String timestampLiteralAfter(Instant snapshotTime)
    {
        return timestampLiteral(snapshotTime.truncatedTo(ChronoUnit.MILLIS).plusMillis(1));
    }

    // ---- 1. Version pins which tables exist. ----

    @Test
    public void testVersionAsOfTpchLoadListsOnlyTablesPresentThen()
    {
        long tpchLoadSnapshot = snapshotIdByCommitMessage("Load TPC-H tiny fixture (nation, region, customer, orders)");

        assertEquals(
                computeActual(format("SELECT count(*) FROM tpch.nation FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", tpchLoadSnapshot)).getOnlyValue(),
                25L);
        assertEquals(
                computeActual(format("SELECT count(*) FROM tpch.orders FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", tpchLoadSnapshot)).getOnlyValue(),
                15000L);

        // types schema exists later (snapshot 3) but its primitives table is created later still
        // (snapshot 4): neither exists yet at the TPC-H load snapshot.
        assertQueryFails(
                format("SELECT 1 FROM types.primitives FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", tpchLoadSnapshot),
                ".*does not exist at DuckLake snapshot.*");
        // del schema itself is created much later: a schema created after the pinned snapshot
        // fails the same way a table-created-later does.
        assertQueryFails(
                format("SELECT 1 FROM del.simple FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", tpchLoadSnapshot),
                ".*does not exist at DuckLake snapshot.*");

        // The same must hold through FOR SYSTEM_TIME AS OF: a timestamp that resolves to the same
        // snapshot must fail the same way, proving the fix applies regardless of which version
        // expression form resolved to it.
        String tpchLoadTimeLiteral = timestampLiteralAfter(snapshotTime(tpchLoadSnapshot));
        assertQueryFails(
                format("SELECT 1 FROM types.primitives FOR SYSTEM_TIME AS OF %s", tpchLoadTimeLiteral),
                ".*does not exist at DuckLake snapshot.*");
        assertQueryFails(
                format("SELECT 1 FROM del.simple FOR SYSTEM_TIME AS OF %s", tpchLoadTimeLiteral),
                ".*does not exist at DuckLake snapshot.*");
    }

    @Test
    public void testVersionAsOfAndBeforeOnFirstInsertIntoPrimitives()
    {
        long firstInsertSnapshot = snapshotIdsByChanges("inserted_into_table:7").get(0);

        assertEquals(
                computeActual(format("SELECT count(*) FROM types.primitives FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", firstInsertSnapshot)).getOnlyValue(),
                1L);
        // The table exists (created at an earlier snapshot) but is still empty strictly before its
        // first insert.
        assertEquals(
                computeActual(format("SELECT count(*) FROM types.primitives FOR SYSTEM_VERSION BEFORE CAST(%d AS BIGINT)", firstInsertSnapshot)).getOnlyValue(),
                0L);
    }

    // ---- 2. Timestamps resolve to the right snapshot. ----

    @Test
    public void testSystemTimeAsOfPicksSnapshotAtOrBeforeGivenTimestamp()
    {
        long insertSnapshot = snapshotIdByChanges("inserted_into_table:16");
        long deleteSnapshot = snapshotIdByChanges("deleted_from_table:16");
        Instant insertTime = snapshotTime(insertSnapshot);
        Instant deleteTime = snapshotTime(deleteSnapshot);

        assertEquals(
                computeActual(format("SELECT count(*) FROM del.simple FOR SYSTEM_TIME AS OF %s", timestampLiteralAfter(insertTime))).getOnlyValue(),
                1000L);
        assertEquals(
                computeActual(format("SELECT count(*) FROM del.simple FOR SYSTEM_TIME AS OF %s", timestampLiteralAfter(deleteTime))).getOnlyValue(),
                900L);
        // A timestamp strictly between the two snapshots resolves to the earlier one.
        String betweenLiteral = timestampLiteral(insertTime.truncatedTo(ChronoUnit.MILLIS).plusMillis(5));
        assertEquals(
                computeActual(format("SELECT count(*) FROM del.simple FOR SYSTEM_TIME AS OF %s", betweenLiteral)).getOnlyValue(),
                1000L);
    }

    @Test
    public void testMergedTableTimeTravelByTimestamp()
    {
        // merge.table: five single-row inserts compacted afterwards into one partial data file;
        // proves timestamp resolution composes with compacted-file filtering.
        long secondInsertSnapshot = snapshotIdsByChanges("inserted_into_table:23").get(1);
        Instant secondInsertTime = snapshotTime(secondInsertSnapshot);
        assertEquals(
                computeActual(format("SELECT count(*) FROM merge.\"table\" FOR SYSTEM_TIME AS OF %s", timestampLiteralAfter(secondInsertTime))).getOnlyValue(),
                2L);
    }

    @Test
    public void testInlinedTableTimeTravelByTimestamp()
    {
        // inl.small: three inlined inserts of 1, 2, 3 rows; proves timestamp resolution composes
        // with inlined-row filtering.
        long secondInlineSnapshot = snapshotIdsByChanges("inlined_insert:21").get(1);
        Instant secondInlineTime = snapshotTime(secondInlineSnapshot);
        assertEquals(
                computeActual(format("SELECT count(*) FROM inl.small FOR SYSTEM_TIME AS OF %s", timestampLiteralAfter(secondInlineTime))).getOnlyValue(),
                3L);
    }

    // ---- 3. Time travel pins data, not schema. ----
    //
    // See the class javadoc: column names/types are always resolved against the table's current
    // (latest) schema by the shared analyzer, regardless of the version/timestamp clause -- this
    // is the same contract Presto's Iceberg connector has, not something DuckLake chose. What a
    // pinned version genuinely controls is which rows come back and what values they carry
    // (including whether a default applies, for a row written before a column existed).

    @Test
    public void testEvolvedTableTimeTravelPinsRowsAndDefaultsNotSchema()
    {
        long firstInsertSnapshot = snapshotIdsByChanges("inserted_into_table:19").get(0);

        // Only "id", "full_name" and "score" (the current, latest column names) can be named in
        // SQL at all, even against this earlier snapshot -- but the DATA they return is pinned:
        // exactly the 2 rows present as of firstInsertSnapshot, "full_name" populated from the
        // file even though it was written under the old name "name", and "score" defaulted to 42
        // because these rows predate the ADD COLUMN that introduced it.
        MaterializedResult result = computeActual(
                format("SELECT id, full_name, score FROM evo.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT) ORDER BY id", firstInsertSnapshot));
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 2);
        assertEquals(rows.get(0).getFields(), List.of(1, "alice", 42));
        assertEquals(rows.get(1).getFields(), List.of(2, "bob", 42));

        assertEquals(
                computeActual(format("SELECT count(*) FROM evo.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", firstInsertSnapshot)).getOnlyValue(),
                2L);
        assertEquals(computeActual("SELECT count(*) FROM evo.\"table\"").getOnlyValue(), 4L);

        // SELECT * expands to the CURRENT column list (id, full_name, score) even at this earlier
        // snapshot, not the 2 columns ("id", "name") that actually existed back then.
        int latestColumnCount = computeActual("SELECT * FROM evo.\"table\"").getTypes().size();
        MaterializedResult star = computeActual(
                format("SELECT * FROM evo.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", firstInsertSnapshot));
        assertEquals(star.getTypes().size(), latestColumnCount);
        assertEquals(star.getRowCount(), 2);
    }

    @Test
    public void testEvolvedTableOldColumnNameUnresolvableEvenAtASnapshotWhereItExisted()
    {
        long firstInsertSnapshot = snapshotIdsByChanges("inserted_into_table:19").get(0);

        // "name" was in fact the live column name as of firstInsertSnapshot (the RENAME COLUMN to
        // "full_name" happens two snapshots later), but it is not a legal identifier in this SQL
        // statement: name resolution runs against the CURRENT schema, which knows only
        // "full_name".
        assertQueryFails(
                format("SELECT name FROM evo.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", firstInsertSnapshot),
                ".*[Cc]olumn 'name' cannot be resolved.*");
    }

    @Test
    public void testEvolvedTableColumnsAtLatestSnapshot()
    {
        assertEquals(computeActual("SELECT full_name FROM evo.\"table\"").getRowCount(), 4);
        assertQueryFails(
                "SELECT name FROM evo.\"table\"",
                ".*[Cc]olumn 'name' cannot be resolved.*");
        assertEquals(computeActual("SELECT count(*) FROM evo.\"table\"").getOnlyValue(), 4L);
    }

    // ---- 4. Before the first snapshot fails clearly. ----

    @Test
    public void testSystemTimeBeforeFirstSnapshotFails()
    {
        assertQueryFails(
                "SELECT 1 FROM tpch.orders FOR SYSTEM_TIME AS OF TIMESTAMP '2000-01-01 00:00:00.000 UTC'",
                ".*No DuckLake snapshot exists at or before.*");
    }

    @Test
    public void testSystemVersionBeforeFirstSnapshotFails()
    {
        assertQueryFails(
                "SELECT 1 FROM tpch.orders FOR SYSTEM_VERSION BEFORE CAST(0 AS BIGINT)",
                ".*No DuckLake snapshot exists before snapshot 0.*");
    }
}
