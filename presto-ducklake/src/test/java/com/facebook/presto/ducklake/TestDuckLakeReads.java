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

import com.facebook.presto.Session;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP;
import static com.facebook.presto.hive.HiveCommonSessionProperties.PARQUET_BATCH_READ_OPTIMIZATION_ENABLED;
import static java.lang.String.format;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

/**
 * End-to-end read-path tests against the {@link TestingDuckLakeCatalog} fixture: the connector's
 * split manager and page source provider are real here (unlike {@link
 * TestDuckLakeMetadataQueries}), so these tests exercise Parquet reading, field-id resolution,
 * default values, and metadata columns ({@code $path}/{@code $row_id}/{@code $row_position}) all
 * the way through a real {@code ducklake} catalog and {@link
 * com.facebook.presto.tests.DistributedQueryRunner}. Positional deletes ({@code del.*}) and
 * inlined data ({@code inl.small}) are both covered below.
 */
public class TestDuckLakeReads
        extends AbstractTestQueryFramework
{
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

    /** Runs TIMESTAMP/TIMESTAMP WITH TIME ZONE assertions without the session zone shift. */
    private Session legacyTimestampFalse()
    {
        return Session.builder(getSession())
                .setSystemProperty(LEGACY_TIMESTAMP, "false")
                .build();
    }

    /**
     * Same as {@link #legacyTimestampFalse()} but also forces the Parquet batch reader path, so
     * the NANOS-to-millis conversion, the UUID byte-order fix, timestamptz, decimals, and nested
     * field-id resolution are all exercised against the batch column decoders too, not just the
     * classic ones.
     */
    private Session batchReaderEnabled()
    {
        return Session.builder(legacyTimestampFalse())
                .setCatalogSessionProperty("ducklake", PARQUET_BATCH_READ_OPTIMIZATION_ENABLED, "true")
                .build();
    }

    @Test
    public void testOrdersCount()
    {
        assertQuery("SELECT count(*) FROM tpch.orders", "SELECT count(*) FROM orders");
    }

    // The free-text comment columns below are deliberately excluded from the cross-checks against
    // H2's TPC-H generator: DuckDB's own tpch extension (which populated this fixture) and
    // Presto's dbgen-based tpch connector (which backs the "SELECT ... FROM nation" side of
    // assertQuery here) each drive comment-text generation off their own, independently valid
    // pseudorandom stream, so the resulting strings never match byte-for-byte between the two
    // even though every structural/numeric/date column coincides exactly. A non-empty sanity
    // check on the comment column stands in for the dropped exact-text comparison.

    @Test
    public void testNationMatchesTpchTiny()
    {
        assertQuery(
                "SELECT n_nationkey, n_name, n_regionkey FROM tpch.nation",
                "SELECT nationkey, name, regionkey FROM nation");
        assertEquals(computeActual("SELECT count(*) FROM tpch.nation WHERE n_comment IS NULL OR n_comment = ''").getOnlyValue(), 0L);
    }

    @Test
    public void testRegionMatchesTpchTiny()
    {
        assertQuery(
                "SELECT r_regionkey, r_name FROM tpch.region",
                "SELECT regionkey, name FROM region");
        assertEquals(computeActual("SELECT count(*) FROM tpch.region WHERE r_comment IS NULL OR r_comment = ''").getOnlyValue(), 0L);
    }

    @Test
    public void testOrdersMatchesTpchTiny()
    {
        assertQuery(
                "SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, " +
                        "o_orderpriority, o_clerk, o_shippriority FROM tpch.orders",
                "SELECT orderkey, custkey, orderstatus, totalprice, orderdate, " +
                        "orderpriority, clerk, shippriority FROM orders");
        assertEquals(computeActual("SELECT count(*) FROM tpch.orders WHERE o_comment IS NULL OR o_comment = ''").getOnlyValue(), 0L);
    }

    @Test
    public void testCustomerCountAndBalance()
    {
        assertQuery("SELECT count(*) FROM tpch.customer", "SELECT count(*) FROM customer");
        assertQuery("SELECT sum(c_acctbal) FROM tpch.customer", "SELECT sum(acctbal) FROM customer");
    }

    @Test
    public void testPrimitivesRowOne()
    {
        assertPrimitivesRowOne(legacyTimestampFalse());
    }

    @Test
    public void testPrimitivesRowOneBatchReader()
    {
        assertPrimitivesRowOne(batchReaderEnabled());
    }

    private void assertPrimitivesRowOne(Session session)
    {
        MaterializedResult result = computeActual(
                session,
                "SELECT " +
                        "bool_col = true, " +
                        "tinyint_col = TINYINT '12', " +
                        "smallint_col = SMALLINT '1234', " +
                        "int_col = INTEGER '123456', " +
                        "bigint_col = BIGINT '1234567890123', " +
                        "float_col = REAL '3.14', " +
                        "double_col = DOUBLE '2.718281828', " +
                        "decimal_col = DECIMAL '12345.678', " +
                        "varchar_col = 'hello world', " +
                        "date_col = DATE '2024-06-15', " +
                        "time_col = TIME '13:45:30', " +
                        "timestamp_col = TIMESTAMP '2024-06-15 13:45:30.123', " +
                        "timestamp_s_col = TIMESTAMP '2024-06-15 13:45:30', " +
                        "timestamp_ms_col = TIMESTAMP '2024-06-15 13:45:30.123', " +
                        "timestamp_ns_col = TIMESTAMP '2024-06-15 13:45:30.123', " +
                        "timestamptz_col = TIMESTAMP '2024-06-15 13:45:30.123 UTC', " +
                        "CAST(uuid_col AS VARCHAR) = '550e8400-e29b-41d4-a716-446655440000', " +
                        "json_extract_scalar(json_col, '$.key') = 'value', " +
                        "blob_col = X'48656C6C6F20576F726C64' " +
                        "FROM types.primitives WHERE id = 1");
        assertEquals(result.getRowCount(), 1);
        MaterializedRow row = result.getMaterializedRows().get(0);
        for (int field = 0; field < row.getFieldCount(); field++) {
            assertEquals(row.getField(field), Boolean.TRUE, "field " + field + " expected true but was " + row.getField(field));
        }
    }

    @Test
    public void testPrimitivesRowTwoIsAllNull()
    {
        assertPrimitivesRowTwoIsAllNull(getSession());
    }

    @Test
    public void testPrimitivesRowTwoIsAllNullBatchReader()
    {
        assertPrimitivesRowTwoIsAllNull(batchReaderEnabled());
    }

    private void assertPrimitivesRowTwoIsAllNull(Session session)
    {
        MaterializedResult result = computeActual(
                session,
                "SELECT bool_col, tinyint_col, smallint_col, int_col, bigint_col, float_col, double_col, " +
                        "decimal_col, varchar_col, date_col, time_col, timestamp_col, timestamp_s_col, " +
                        "timestamp_ms_col, timestamp_ns_col, timestamptz_col, uuid_col, json_col, blob_col " +
                        "FROM types.primitives WHERE id = 2");
        assertEquals(result.getRowCount(), 1);
        MaterializedRow row = result.getMaterializedRows().get(0);
        for (int field = 0; field < row.getFieldCount(); field++) {
            assertNull(row.getField(field), "field " + field + " expected null but was " + row.getField(field));
        }
    }

    @Test
    public void testNestedRowOne()
    {
        assertNestedRowOne(getSession());
    }

    @Test
    public void testNestedRowOneBatchReader()
    {
        assertNestedRowOne(batchReaderEnabled());
    }

    private void assertNestedRowOne(Session session)
    {
        MaterializedResult result = computeActual(
                session,
                "SELECT struct_col.a, struct_col.b, list_col[1], cardinality(list_col), " +
                        "map_col['x'], cardinality(map_col), struct_with_list.name, struct_with_list.tags[2] " +
                        "FROM types.nested WHERE id = 1");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(row.getField(0), 10);
        assertEquals(row.getField(1), "ten");
        assertEquals(row.getField(2), 1);
        assertEquals(row.getField(3), 3L);
        assertEquals(row.getField(4), 1);
        assertEquals(row.getField(5), 2L);
        assertEquals(row.getField(6), "first");
        assertEquals(row.getField(7), "b");
    }

    @Test
    public void testNestedRowTwoIsEmpty()
    {
        MaterializedResult result = computeActual(
                "SELECT cardinality(list_col), struct_col.a, struct_col.b, cardinality(map_col), " +
                        "struct_with_list.name, cardinality(struct_with_list.tags) " +
                        "FROM types.nested WHERE id = 2");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(row.getField(0), 0L);
        assertNull(row.getField(1));
        assertNull(row.getField(2));
        assertEquals(row.getField(3), 0L);
        assertEquals(row.getField(4), "second");
        assertEquals(row.getField(5), 0L);
    }

    @Test
    public void testNestedRowThreeIsAllNull()
    {
        MaterializedResult result = computeActual(
                "SELECT list_col IS NULL, struct_col IS NULL, map_col IS NULL, struct_with_list IS NULL " +
                        "FROM types.nested WHERE id = 3");
        MaterializedRow row = result.getMaterializedRows().get(0);
        for (int field = 0; field < row.getFieldCount(); field++) {
            assertEquals(row.getField(field), Boolean.TRUE);
        }
    }

    @Test
    public void testEvolvedTableReturnsDefaultAndRenamedColumn()
    {
        assertEvolvedTableReturnsDefaultAndRenamedColumn(getSession());
    }

    @Test
    public void testEvolvedTableReturnsDefaultAndRenamedColumnBatchReader()
    {
        assertEvolvedTableReturnsDefaultAndRenamedColumn(batchReaderEnabled());
    }

    private void assertEvolvedTableReturnsDefaultAndRenamedColumn(Session session)
    {
        MaterializedResult result = computeActual(session, "SELECT id, full_name, score FROM evo.\"table\" ORDER BY id");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 4);
        assertEquals(rows.get(0).getFields(), List.of(1, "alice", 42));
        assertEquals(rows.get(1).getFields(), List.of(2, "bob", 42));
        assertEquals(rows.get(2).getFields(), List.of(3, "carol", 100));
        assertEquals(rows.get(3).getFields(), List.of(4, "dave", 200));
    }

    @Test
    public void testEvolvedTableDefaultCount()
    {
        assertEquals(computeActual("SELECT count(*) FROM evo.\"table\" WHERE score = 42").getOnlyValue(), 2L);
    }

    @Test
    public void testMergedTableIgnoresHiddenSnapshotColumn()
    {
        MaterializedResult result = computeActual("SELECT id, val FROM merge.\"table\" ORDER BY id");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 5);
        assertEquals(rows.get(0).getFields(), List.of(1, "a"));
        assertEquals(rows.get(1).getFields(), List.of(2, "b"));
        assertEquals(rows.get(2).getFields(), List.of(3, "c"));
        assertEquals(rows.get(3).getFields(), List.of(4, "d"));
        assertEquals(rows.get(4).getFields(), List.of(5, "e"));
    }

    @Test
    public void testPartitionPruningByIdentity()
    {
        assertEquals(computeActual("SELECT count(*) FROM part.by_identity WHERE id = 3").getOnlyValue(), 10L);
    }

    @Test
    public void testPartitionPruningByMonth()
    {
        MaterializedResult result = computeActual(
                legacyTimestampFalse(),
                "SELECT count(*) FROM part.by_month WHERE ts >= TIMESTAMP '2025-01-01 00:00:00'");
        assertEquals(result.getOnlyValue(), 34L);
    }

    @Test
    public void testOrdersPredicatePushdownMatchesTpchTiny()
    {
        assertQuery(
                "SELECT count(*) FROM tpch.orders WHERE o_orderkey < 10",
                "SELECT count(*) FROM orders WHERE orderkey < 10");
    }

    @Test
    public void testRowIdAndPathMetadataColumns()
    {
        MaterializedResult result = computeActual(
                "SELECT min(\"$row_id\"), max(\"$row_id\"), count(DISTINCT \"$path\") FROM tpch.orders");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(row.getField(0), 0L);
        assertEquals(row.getField(1), 14999L);
        assertEquals(row.getField(2), 1L);
    }

    @Test
    public void testRowIdAndRowPositionAgreeOnFirstRows()
    {
        MaterializedResult result = computeActual(
                "SELECT \"$row_id\", \"$row_position\", n_nationkey FROM tpch.nation ORDER BY \"$row_id\" LIMIT 3");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 3);
        for (int i = 0; i < 3; i++) {
            assertEquals(rows.get(i).getFields(), List.of((long) i, (long) i, i));
        }
    }

    @Test
    public void testPathColumnEndsWithParquet()
    {
        String path = (String) computeActual("SELECT \"$path\" FROM tpch.nation LIMIT 1").getOnlyValue();
        assertTrue(path.endsWith(".parquet"), "expected a .parquet path but got " + path);
    }

    @Test
    public void testSimpleDeleteFileFiltersOutDeletedRange()
    {
        assertEquals(computeActual("SELECT count(*) FROM del.simple").getOnlyValue(), 900L);
        assertEquals(computeActual("SELECT count(*) FROM del.simple WHERE id BETWEEN 100 AND 199").getOnlyValue(), 0L);
        MaterializedResult result = computeActual("SELECT min(id), max(id), count(DISTINCT id) FROM del.simple");
        assertEquals(result.getMaterializedRows().get(0).getFields(), List.of(0, 999, 900L));
    }

    @Test
    public void testSimpleDeleteFileBatchReader()
    {
        assertEquals(computeActual(batchReaderEnabled(), "SELECT count(*) FROM del.simple").getOnlyValue(), 900L);
    }

    @Test
    public void testSimpleDeleteFileRowIdAndRowPositionStayCorrect()
    {
        // $row_id (rowIdStart + physical position) must be filtered exactly like $row_position:
        // proves the delete filter still lines up after the position channel is applied.
        assertEquals(computeActual("SELECT count(*) FROM del.simple WHERE \"$row_id\" BETWEEN 100 AND 199").getOnlyValue(), 0L);
        assertEquals(computeActual("SELECT count(*) FROM del.simple WHERE \"$row_position\" < 100").getOnlyValue(), 100L);
    }

    @Test
    public void testTwoSnapshotsDeleteFileAppliesLatestState()
    {
        assertEquals(computeActual("SELECT count(*) FROM del.two_snapshots").getOnlyValue(), 800L);
        assertEquals(computeActual("SELECT count(*) FROM del.two_snapshots WHERE id < 100 OR id BETWEEN 500 AND 599").getOnlyValue(), 0L);
    }

    @Test
    public void testTwoSnapshotsDeleteFileTimeTravel()
    {
        // Derived from the fixture's own snapshot history rather than hardcoded, per del.two_snapshots'
        // two DELETE commits (ids 0-99, then 500-599), each recorded as its own "deleted_from_table:17"
        // snapshot-changes row.
        List<MaterializedRow> deleteSnapshots = computeActual(
                "SELECT snapshot_id FROM del.\"two_snapshots$snapshots\" WHERE changes = 'deleted_from_table:17' ORDER BY snapshot_id")
                .getMaterializedRows();
        assertEquals(deleteSnapshots.size(), 2);
        long firstDeleteSnapshot = (Long) deleteSnapshots.get(0).getField(0);
        long secondDeleteSnapshot = (Long) deleteSnapshots.get(1).getField(0);

        assertEquals(countAtSnapshot(firstDeleteSnapshot - 1), 1000L);
        assertEquals(countAtSnapshot(firstDeleteSnapshot), 900L);
        assertEquals(countAtSnapshot(secondDeleteSnapshot), 800L);
    }

    private Object countAtSnapshot(long snapshotId)
    {
        return computeActual(format("SELECT count(*) FROM del.two_snapshots FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", snapshotId)).getOnlyValue();
    }

    @Test
    public void testMergedTableCountAtLatestSnapshotIsSumOfInserts()
    {
        assertEquals(computeActual("SELECT count(*) FROM merge.\"table\"").getOnlyValue(), 5L);
    }

    @Test
    public void testMergedTableCountAtLatestSnapshotIsSumOfInsertsBatchReader()
    {
        assertEquals(computeActual(batchReaderEnabled(), "SELECT count(*) FROM merge.\"table\"").getOnlyValue(), 5L);
    }

    @Test
    public void testMergedTableTimeTravelHidesRowsFromLaterSnapshots()
    {
        // merge.table is five single-row inserts, one per snapshot, compacted afterwards into one
        // partial data file (partial_max = the fifth insert's snapshot). Derived from the
        // fixture's own snapshot history rather than hardcoded, per each insert's own
        // "inserted_into_table:23" snapshot-changes row.
        List<MaterializedRow> insertSnapshots = computeActual(
                "SELECT snapshot_id FROM merge.\"table$snapshots\" WHERE changes = 'inserted_into_table:23' ORDER BY snapshot_id")
                .getMaterializedRows();
        assertEquals(insertSnapshots.size(), 5);
        long firstInsertSnapshot = (Long) insertSnapshots.get(0).getField(0);
        long secondInsertSnapshot = (Long) insertSnapshots.get(1).getField(0);
        long fifthInsertSnapshot = (Long) insertSnapshots.get(4).getField(0);

        assertEquals(countMergedTableAtSnapshot(firstInsertSnapshot), 1L);
        assertEquals(countMergedTableAtSnapshot(secondInsertSnapshot), 2L);
        assertEquals(countMergedTableAtSnapshot(fifthInsertSnapshot), 5L);

        MaterializedResult idsAtSecondSnapshot = computeActual(
                format("SELECT id FROM merge.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT) ORDER BY id", secondInsertSnapshot));
        List<MaterializedRow> rows = idsAtSecondSnapshot.getMaterializedRows();
        assertEquals(rows.size(), 2);
        assertEquals(rows.get(0).getFields(), List.of(1));
        assertEquals(rows.get(1).getFields(), List.of(2));

        MaterializedResult rowIdsAtSecondSnapshot = computeActual(
                format("SELECT \"$row_id\" FROM merge.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT) ORDER BY 1", secondInsertSnapshot));
        List<MaterializedRow> rowIdRows = rowIdsAtSecondSnapshot.getMaterializedRows();
        assertEquals(rowIdRows.size(), 2);
        assertEquals(rowIdRows.get(0).getFields(), List.of(0L));
        assertEquals(rowIdRows.get(1).getFields(), List.of(1L));
    }

    @Test
    public void testMergedTableTimeTravelBatchReader()
    {
        List<MaterializedRow> insertSnapshots = computeActual(
                "SELECT snapshot_id FROM merge.\"table$snapshots\" WHERE changes = 'inserted_into_table:23' ORDER BY snapshot_id")
                .getMaterializedRows();
        long secondInsertSnapshot = (Long) insertSnapshots.get(1).getField(0);
        assertEquals(
                computeActual(batchReaderEnabled(), format("SELECT count(*) FROM merge.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", secondInsertSnapshot))
                        .getOnlyValue(),
                2L);
    }

    private Object countMergedTableAtSnapshot(long snapshotId)
    {
        return computeActual(format("SELECT count(*) FROM merge.\"table\" FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", snapshotId)).getOnlyValue();
    }

    @Test
    public void testInlinedTableCountIsInlinedPlusFlushedRows()
    {
        // inl.small: three inlined inserts (1+2+3 = 6 rows, never flushed to Parquet because they
        // stay under data_inlining_row_limit) plus one 20-row insert flushed as its own file.
        assertEquals(computeActual("SELECT count(*) FROM inl.small").getOnlyValue(), 26L);
    }

    @Test
    public void testInlinedTableCountIsInlinedPlusFlushedRowsBatchReader()
    {
        assertEquals(computeActual(batchReaderEnabled(), "SELECT count(*) FROM inl.small").getOnlyValue(), 26L);
    }

    @Test
    public void testInlinedTableTimeTravelSeesOnlyRowsInlinedSoFar()
    {
        // Derived from the fixture's own snapshot history rather than hardcoded, per each inlined
        // insert's own "inlined_insert:21" snapshot-changes row.
        List<MaterializedRow> inlineSnapshots = computeActual(
                "SELECT snapshot_id FROM inl.\"small$snapshots\" WHERE changes = 'inlined_insert:21' ORDER BY snapshot_id")
                .getMaterializedRows();
        assertEquals(inlineSnapshots.size(), 3);
        long firstInlineSnapshot = (Long) inlineSnapshots.get(0).getField(0);
        long secondInlineSnapshot = (Long) inlineSnapshots.get(1).getField(0);
        long thirdInlineSnapshot = (Long) inlineSnapshots.get(2).getField(0);

        assertEquals(countInlinedTableAtSnapshot(firstInlineSnapshot), 1L);
        assertEquals(countInlinedTableAtSnapshot(secondInlineSnapshot), 3L);
        assertEquals(countInlinedTableAtSnapshot(thirdInlineSnapshot), 6L);
    }

    private Object countInlinedTableAtSnapshot(long snapshotId)
    {
        return computeActual(format("SELECT count(*) FROM inl.small FOR SYSTEM_VERSION AS OF CAST(%d AS BIGINT)", snapshotId)).getOnlyValue();
    }

    @Test
    public void testInlinedTableRowIdsAreUniqueAcrossParquetAndInlinedSplits()
    {
        MaterializedResult result = computeActual("SELECT count(DISTINCT \"$row_id\"), min(\"$row_id\"), max(\"$row_id\") FROM inl.small");
        MaterializedRow row = result.getMaterializedRows().get(0);
        assertEquals(row.getFields(), List.of(26L, 0L, 25L));
    }

    @Test
    public void testInlinedTablePathIsNullOnlyForInlinedRows()
    {
        assertEquals(computeActual("SELECT count(*) FROM inl.small WHERE \"$path\" IS NULL").getOnlyValue(), 6L);
        assertEquals(computeActual("SELECT count(*) FROM inl.small WHERE \"$path\" IS NOT NULL").getOnlyValue(), 20L);
    }

    @Test
    public void testInlinedTablePredicateOnInlinedColumnFiltersCorrectly()
    {
        // Filtering happens in the engine (no pushdown to the inlined table read), so this
        // exercises the connector's ability to project a regular column and hand it to the
        // engine's own filter: id = 5 matches once from the bulk (Parquet) insert and once from
        // the inlined inserts.
        MaterializedResult result = computeActual("SELECT val FROM inl.small WHERE id = 5 ORDER BY val");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 2);
        assertEquals(rows.get(0).getFields(), List.of("bulk_5"));
        assertEquals(rows.get(1).getFields(), List.of("e"));
    }

    @Test
    public void testInlinedTableRowIdOrderingMatchesInsertOrder()
    {
        MaterializedResult result = computeActual("SELECT id, val FROM inl.small WHERE \"$row_id\" < 6 ORDER BY id");
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), 6);
        String[] expectedVals = {"a", "b", "c", "d", "e", "f"};
        for (int i = 0; i < 6; i++) {
            assertEquals(rows.get(i).getFields(), List.of(i + 1, expectedVals[i]));
        }
    }
}
