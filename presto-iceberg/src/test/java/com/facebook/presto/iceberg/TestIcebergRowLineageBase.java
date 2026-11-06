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
package com.facebook.presto.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.OverwriteFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopInputFile;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for Iceberg Format Version 3 row lineage ({@code _row_id} / {@code _last_updated_sequence_number}).
 *
 * Tables are written through the Iceberg API, because Presto DML cannot produce stored lineage values,
 * compaction output, or a V2 to V3 upgrade.
 */
public abstract class TestIcebergRowLineageBase
        extends AbstractTestQueryFramework
{
    protected static final String TEST_SCHEMA = "tpch";
    private static final Schema TEST_TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));

    protected abstract File getCatalogDirectory();

    @DataProvider(name = "pushdownFilterEnabled")
    public Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {{false}};
    }

    /**
     * Checks {@code sql} under {@code session} against a second engine. The base class has none, so
     * this does nothing.
     */
    protected void assertMatchesExpectedEngine(Session session, String sql)
    {
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testV3TableRowLineageMatchesIcebergMetadata(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_row_lineage";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            Schema schema = table.schema();

            writeRecords(table, GenericRecord.create(schema).copy("id", 1, "value", "one"));
            table.refresh();
            writeRecords(table, GenericRecord.create(schema).copy("id", 2, "value", "two"));

            table.refresh();
            List<long[]> expectedPairs = buildExpectedPairs(table, "Iceberg should set firstRowId for V3 tables");

            assertPrestoRowLineageMatchesExpected(session, tableName, expectedPairs);

            assertScalar(session, "SELECT count(DISTINCT \"_row_id\") FROM " + tableName,
                    2L, "Row IDs must be unique across all rows");
            assertScalar(session, "SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + tableName,
                    2L, "Sequence numbers should differ between commits");

            String sequenceNumberForIdSql = "SELECT \"_last_updated_sequence_number\" FROM " + tableName + " WHERE id = ";
            assertMatchesExpectedEngine(session, sequenceNumberForIdSql + 1);
            assertMatchesExpectedEngine(session, sequenceNumberForIdSql + 2);
            Long seqForFirst = (Long) computeScalar(session, sequenceNumberForIdSql + 1);
            Long seqForSecond = (Long) computeScalar(session, sequenceNumberForIdSql + 2);
            assertTrue(seqForFirst < seqForSecond,
                    "_last_updated_sequence_number should be smaller for earlier commits");
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testV3TableRowLineageWithMultipleRowsPerCommit(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_row_lineage_multi";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            Schema schema = table.schema();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"),
                    GenericRecord.create(schema).copy("id", 3, "value", "three"));

            table.refresh();
            List<long[]> expectedPairs = buildExpectedPairs(table, "firstRowId should be set for V3 tables");

            assertPrestoRowLineageMatchesExpected(session, tableName, expectedPairs);

            long sharedSeqNum = expectedPairs.get(0)[1];
            for (long[] pair : expectedPairs) {
                assertEquals(pair[1], sharedSeqNum,
                        "All rows in a single commit should have the same sequence number");
            }

            assertScalar(session, "SELECT count(DISTINCT \"_row_id\") FROM " + tableName,
                    3L, "Row IDs must be unique across all rows");
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testRowLineageBackfilledOnV2ToV3Upgrade(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_row_lineage_v2_to_v3";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "2");
            Schema schema = table.schema();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"));
            table.refresh();
            writeRecords(table, GenericRecord.create(schema).copy("id", 3, "value", "three"));

            // V2 tables have no row lineage; both columns are null.
            String allRowsSql = "SELECT \"_row_id\", * FROM " + tableName;
            assertMatchesExpectedEngine(session, allRowsSql);
            assertEquals(computeActual(session, allRowsSql).getRowCount(), 3);
            assertScalar(session, "SELECT count(*) FROM " + tableName + " WHERE \"_row_id\" IS NOT NULL",
                    0L, "_row_id should be null for all rows in a V2 table");
            assertScalar(session, "SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NOT NULL",
                    0L, "_last_updated_sequence_number should be null for all rows in a V2 table");

            table.refresh();
            table.updateProperties().set("format-version", "3").commit();
            table.refresh();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 4, "value", "four"),
                    GenericRecord.create(schema).copy("id", 5, "value", "five"));
            table.refresh();

            assertScalar(session, "SELECT count(*) FROM " + tableName + " WHERE \"_row_id\" IS NULL",
                    0L, "All rows should have non-null _row_id after V3 upgrade");
            assertScalar(session, "SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NULL",
                    0L, "All rows should have non-null _last_updated_sequence_number after V3 upgrade");

            assertScalar(session, "SELECT count(DISTINCT \"_row_id\") FROM " + tableName,
                    5L, "Row IDs must be unique across all 5 rows after upgrade");

            table.refresh();
            List<long[]> allExpectedPairs = buildExpectedPairs(table,
                    "All files should have firstRowId set after V3 upgrade");
            assertPrestoRowLineageMatchesExpected(session, tableName, allExpectedPairs);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * A row whose {@code _row_id} / {@code _last_updated_sequence_number} are stored in the data
     * file, as an external row-preserving UPDATE/MERGE writes them, reports those stored values
     * whether or not the query also filters on the column.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueries(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_row_lineage_predicate_vs_projection";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            writeRecords(table, GenericRecord.create(table.schema()).copy("id", 1, "value", "one"));
            table.refresh();

            writeOverriddenLineageRow(table, 2, "two");
            table.refresh();

            String unfilteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName;
            String filteredSql = unfilteredSql + " WHERE \"_row_id\" IS NOT NULL";
            assertMatchesExpectedEngine(session, unfilteredSql);
            assertMatchesExpectedEngine(session, filteredSql);

            MaterializedResult unfiltered = computeActual(session, unfilteredSql);
            MaterializedResult filtered = computeActual(session, filteredSql);

            assertEquals(unfiltered.getRowCount(), 2);
            Map<Integer, long[]> unfilteredById = rowIdAndSequenceNumberById(unfiltered);
            Map<Integer, long[]> filteredById = rowIdAndSequenceNumberById(filtered);
            assertEquals(unfilteredById.keySet(), filteredById.keySet(),
                    "plain projection and predicate-filtered queries must see the same rows");
            for (Integer id : unfilteredById.keySet()) {
                assertEquals(unfilteredById.get(id)[0], filteredById.get(id)[0],
                        "_row_id must match between unfiltered and filtered queries for id=" + id);
                assertEquals(unfilteredById.get(id)[1], filteredById.get(id)[1],
                        "_last_updated_sequence_number must match between unfiltered and filtered queries for id=" + id);
            }

            assertEquals(unfilteredById.get(2)[0], 42L,
                    "_row_id should reflect the file's explicit physical value, not firstRowId+position");
            assertEquals(unfilteredById.get(2)[1], 99L,
                    "_last_updated_sequence_number should reflect the file's explicit physical value, not the file's dataSequenceNumber");
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testRowIdPredicatesOnInheritedValues(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_row_id_inherited";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            appendIdAndValue(table, 3, "three");

            // One row per commit, committed in id order, so ascending row ids map to ids 1, 2, 3.
            List<long[]> expectedPairs = buildExpectedPairs(table, "firstRowId should be set for V3 tables");
            long rowId1 = expectedPairs.get(0)[0];
            long rowId2 = expectedPairs.get(1)[0];
            long rowId3 = expectedPairs.get(2)[0];
            assertEquals(readRowIdsById(session, tableName), ImmutableMap.of(1, rowId1, 2, rowId2, 3, rowId3));

            assertIdsForPredicate(session, tableName, "_row_id", "= " + rowId2, ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_row_id", "< " + rowId3, ImmutableList.of(1, 2));
            assertIdsForPredicate(session, tableName, "_row_id", "IN (" + rowId1 + ", " + rowId3 + ")", ImmutableList.of(1, 3));
            assertIdsForPredicate(session, tableName, "_row_id", "> " + rowId3, ImmutableList.of());
            assertIdsForQuery(session, "SELECT id FROM " + tableName +
                            " WHERE \"_row_id\" = " + rowId1 + " OR \"_row_id\" = " + rowId3,
                    ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testRowIdPredicatesOnStoredValues(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_row_id_stored";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            writeRecords(table, GenericRecord.create(table.schema()).copy("id", 1, "value", "one"));
            table.refresh();
            writeOverriddenLineageRow(table, 2, "two");
            table.refresh();

            DataFile storedFile = dataFiles(table).stream()
                    .filter(file -> file.lowerBounds() != null && file.lowerBounds().containsKey(MetadataColumns.ROW_ID.fieldId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("file with stored lineage not found"));
            long inheritedRowId = storedFile.firstRowId();
            long inheritedSequenceNumber = storedFile.dataSequenceNumber();

            assertIdsForPredicate(session, tableName, "_row_id", "= 42", ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_row_id", "= " + inheritedRowId, ImmutableList.of());
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= 99", ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= " + inheritedSequenceNumber, ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testLineagePredicateCombinedWithRegularColumn(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_combined_predicates";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            appendIdAndValue(table, 3, "three");
            Map<Integer, Long> rowIdsById = readRowIdsById(session, tableName);
            long rowId1 = rowIdsById.get(1);
            long sequenceNumber2 = readSequenceNumbersById(session, tableName).get(2);
            List<Integer> idsWithEvenRowId = rowIdsById.entrySet().stream()
                    .filter(entry -> entry.getValue() % 2 == 0)
                    .map(Map.Entry::getKey)
                    .collect(toImmutableList());

            assertIdsForQuery(session, "SELECT id FROM " + tableName +
                            " WHERE \"_last_updated_sequence_number\" >= " + sequenceNumber2 + " AND value <> 'three'",
                    ImmutableList.of(2));
            assertIdsForQuery(session, "SELECT id FROM " + tableName +
                            " WHERE \"_row_id\" = " + rowId1 + " OR id = 3",
                    ImmutableList.of(1, 3));
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" % 2 = 0",
                    idsWithEvenRowId);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    protected Session sessionWithPushdown(boolean pushdownFilterEnabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
    }

    private void assertScalar(Session session, String sql, Object expected, String message)
    {
        assertMatchesExpectedEngine(session, sql);
        assertEquals(computeScalar(session, sql), expected, message);
    }

    protected void assertPrestoRowLineageMatchesExpected(Session session, String tableName, List<long[]> expectedPairs)
    {
        String sql = "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName + " ORDER BY \"_row_id\"";
        assertMatchesExpectedEngine(session, sql);
        MaterializedResult result = computeActual(session, sql);
        List<MaterializedRow> rows = result.getMaterializedRows();
        assertEquals(rows.size(), expectedPairs.size(),
                "Presto and Iceberg API should return the same number of rows");
        for (int i = 0; i < rows.size(); i++) {
            Long rowId = (Long) rows.get(i).getField(0);
            Long seqNum = (Long) rows.get(i).getField(1);
            assertNotNull(rowId, "Presto _row_id should not be null for V3 table");
            assertNotNull(seqNum, "Presto _last_updated_sequence_number should not be null");
            assertEquals(rowId.longValue(), expectedPairs.get(i)[0],
                    "_row_id should match Iceberg metadata");
            assertEquals(seqNum.longValue(), expectedPairs.get(i)[1],
                    "_last_updated_sequence_number should match Iceberg metadata");
        }
    }

    /**
     * Derives the {@code (_row_id, _last_updated_sequence_number)} pairs the table should report
     * from the Iceberg metadata alone: each row's id is its file's {@code firstRowId} plus its
     * position, and its sequence number is the file's {@code dataSequenceNumber}. Sorted by row id.
     */
    protected static List<long[]> buildExpectedPairs(Table table, String firstRowIdMessage)
            throws Exception
    {
        List<long[]> pairs = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile dataFile = task.file();
                Long firstRowId = dataFile.firstRowId();
                assertNotNull(firstRowId, firstRowIdMessage);
                assertTrue(firstRowId >= 0, "firstRowId should be non-negative: " + firstRowId);
                long seqNum = dataFile.dataSequenceNumber();
                for (long pos = 0; pos < dataFile.recordCount(); pos++) {
                    pairs.add(new long[] {firstRowId + pos, seqNum});
                }
            }
        }
        pairs.sort((a, b) -> Long.compare(a[0], b[0]));
        return pairs;
    }

    protected static Table createTestTable(Catalog catalog, TableIdentifier tableId, String formatVersion)
    {
        return catalog.createTable(tableId, TEST_TABLE_SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", formatVersion));
    }

    /**
     * Drops a test table during teardown. A failure to drop is ignored so that it can never replace
     * the assertion error that actually failed the test.
     */
    protected static void dropTableQuietly(Catalog catalog, TableIdentifier tableId)
    {
        try {
            catalog.dropTable(tableId, true);
        }
        catch (Exception ignored) {
        }
    }

    protected void writeRecords(Table table, Record... records)
            throws Exception
    {
        writeRecordsWithSchema(table, table.schema(), records);
    }

    protected Catalog loadCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                getProperties(), new Configuration());
    }

    private Map<String, String> getProperties()
    {
        return ImmutableMap.of("warehouse", getCatalogDirectory().toURI().toString());
    }

    /**
     * Only {@code id} is selected, so {@code column} is read for the filter alone.
     */
    private void assertIdsForPredicate(Session session, String tableName, String column, String predicate, List<Integer> expectedIds)
    {
        assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"" + column + "\" " + predicate, expectedIds);
    }

    /**
     * Compares {@code sql} under {@code session} with the expected engine, then checks the ids
     * against {@code expectedIds}. That catches errors both engines share, such as coordinator
     * split pruning.
     */
    private void assertIdsForQuery(Session session, String sql, List<Integer> expectedIds)
    {
        assertMatchesExpectedEngine(session, sql);
        List<Integer> actualIds = computeActual(session, sql).getOnlyColumn()
                .map(Integer.class::cast)
                .sorted()
                .collect(toImmutableList());
        assertEquals(actualIds, ImmutableList.sortedCopyOf(expectedIds), "rows for \"" + sql + "\"");
    }

    /**
     * Reads {@code _last_updated_sequence_number} for every row of a table created by
     * {@link #createTestTable}, keyed by its {@code id} column.
     */
    private Map<Integer, Long> readSequenceNumbersById(Session session, String tableName)
    {
        return readLineageColumnById(session, tableName, "_last_updated_sequence_number");
    }

    /**
     * Reads {@code _row_id} for every row of a table created by {@link #createTestTable}, keyed by
     * its {@code id} column.
     */
    private Map<Integer, Long> readRowIdsById(Session session, String tableName)
    {
        return readLineageColumnById(session, tableName, "_row_id");
    }

    /**
     * Reads the BIGINT lineage {@code column} for every row of a table created by
     * {@link #createTestTable}, keyed by its {@code id} column.
     */
    private Map<Integer, Long> readLineageColumnById(Session session, String tableName, String column)
    {
        String sql = "SELECT id, \"" + column + "\" FROM " + tableName;
        assertMatchesExpectedEngine(session, sql);
        return computeActual(session, sql).getMaterializedRows().stream()
                .collect(toImmutableMap(row -> (Integer) row.getField(0), row -> (Long) row.getField(1)));
    }

    /**
     * Expects {@code result} to have the columns {@code (id, _row_id, _last_updated_sequence_number)}
     * and returns {@code [_row_id, _last_updated_sequence_number]} keyed by {@code id}.
     */
    private static Map<Integer, long[]> rowIdAndSequenceNumberById(MaterializedResult result)
    {
        Map<Integer, long[]> byId = new HashMap<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            int id = (Integer) row.getField(0);
            Long rowId = (Long) row.getField(1);
            Long seqNum = (Long) row.getField(2);
            assertNotNull(rowId, "_row_id should not be null for id=" + id);
            assertNotNull(seqNum, "_last_updated_sequence_number should not be null for id=" + id);
            byId.put(id, new long[] {rowId, seqNum});
        }
        return byId;
    }

    /**
     * Appends the {@code (id, value)} row to a table created by {@link #createTestTable} in its own
     * commit.
     */
    private void appendIdAndValue(Table table, int id, String value)
            throws Exception
    {
        writeRecords(table, GenericRecord.create(table.schema()).copy("id", id, "value", value));
        table.refresh();
    }

    private static void writeRecordsWithSchema(Table table, Schema writeSchema, Record... records)
            throws Exception
    {
        table.newAppend().appendFile(writeFile(table, writeSchema, records)).commit();
    }

    /**
     * Appends the {@code (id, value)} row to a table created by {@link #createTestTable}, then
     * simulates a row-preserving external copy-on-write UPDATE of it: the file holding the row is
     * replaced by one that explicitly carries {@code _row_id} / {@code _last_updated_sequence_number}
     * values that override the positional and file-level fallbacks.
     */
    private static void writeOverriddenLineageRow(Table table, int id, String value)
            throws Exception
    {
        DataFile original = writeFile(table, table.schema(), GenericRecord.create(table.schema()).copy("id", id, "value", value));
        table.newAppend().appendFile(original).commit();

        Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
        table.newOverwrite()
                .deleteFile(original)
                .addFile(writeFile(table, lineageSchema, lineageRecord(lineageSchema, id, value + "-updated", 42L, 99L)))
                .commit();
    }

    private static List<DataFile> dataFiles(Table table)
            throws Exception
    {
        List<DataFile> files = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
            for (FileScanTask task : tasks) {
                files.add(task.file());
            }
        }
        return files;
    }

    private static DataFile writeFile(Table table, Schema writeSchema, Record... records)
            throws Exception
    {
        return writeFileWithMetrics(table, writeSchema, MetricsConfig.forTable(table), records);
    }

    private static DataFile writeFileWithMetrics(Table table, Schema writeSchema, MetricsConfig metricsConfig, Record... records)
            throws Exception
    {
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(table.location(), "data/data-" + UUID.randomUUID() + ".parquet");
        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(filePath, new Configuration()))
                .schema(writeSchema)
                .withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create)
                .metricsConfig(metricsConfig)
                .overwrite()
                .build();
        try (Closeable ignored = writer) {
            for (Record record : records) {
                writer.write(record);
            }
        }
        return writer.toDataFile();
    }

    /**
     * Returns a {@code lineageSchema} record that stores {@code rowId} and {@code sequenceNumber},
     * or inherits them when they are null.
     */
    private static Record lineageRecord(Schema lineageSchema, int id, String value, Long rowId, Long sequenceNumber)
    {
        Record record = GenericRecord.create(lineageSchema);
        record.setField("id", id);
        record.setField("value", value);
        record.setField(MetadataColumns.ROW_ID.name(), rowId);
        record.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), sequenceNumber);
        return record;
    }

    /**
     * Rewrites every data file of a table created by {@link #createTestTable} into one Parquet file
     * that stores each row's current {@code _row_id} and {@code _last_updated_sequence_number}, as a
     * lineage-preserving compaction does, and returns the new file. Its metrics follow
     * {@code metricsConfig}.
     */
    private DataFile compactPreservingLineage(Session session, Table table, String tableName, MetricsConfig metricsConfig)
            throws Exception
    {
        String sql = "SELECT id, value, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName;
        assertMatchesExpectedEngine(session, sql);
        Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
        Record[] records = computeActual(session, sql).getMaterializedRows().stream()
                .map(row -> lineageRecord(lineageSchema, (Integer) row.getField(0), (String) row.getField(1), (Long) row.getField(2), (Long) row.getField(3)))
                .toArray(Record[]::new);
        DataFile compacted = writeFileWithMetrics(table, lineageSchema, metricsConfig, records);
        table.newRewrite()
                .rewriteFiles(ImmutableSet.copyOf(dataFiles(table)), ImmutableSet.of(compacted))
                .commit();
        table.refresh();
        return getOnlyElement(dataFiles(table));
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testPredicatePushdownPreCompaction(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_pushdown_pre";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            appendIdAndValue(table, 3, "three");

            Map<Integer, Long> sequenceNumbers = readSequenceNumbersById(session, tableName);
            assertEquals(sequenceNumbers.size(), 3);
            long seq1 = sequenceNumbers.get(1);
            long seq2 = sequenceNumbers.get(2);
            long seq3 = sequenceNumbers.get(3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            assertIdsForSequenceNumberPredicate(session, tableName, "<= " + seq1, ImmutableList.of(1));
            assertIdsForSequenceNumberPredicate(session, tableName, "<= " + seq2, ImmutableList.of(1, 2));
            assertIdsForSequenceNumberPredicate(session, tableName, "<= " + seq3, ImmutableList.of(1, 2, 3));
            assertIdsForSequenceNumberPredicate(session, tableName, "< " + seq1, ImmutableList.of());
            assertIdsForSequenceNumberPredicate(session, tableName, "BETWEEN " + seq2 + " AND " + seq3, ImmutableList.of(2, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testPredicatePushdownPostCompaction(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_pushdown_post";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            Map<Integer, Long> preSequenceNumbers = readSequenceNumbersById(session, tableName);
            long preSeq1 = preSequenceNumbers.get(1);
            long preSeq2 = preSequenceNumbers.get(2);
            assertTrue(preSeq1 < preSeq2);

            assertEquals(dataFiles(table).size(), 2);
            DataFile committedFile = compactPreservingLineage(session, table, tableName, MetricsConfig.forTable(table));

            Map<Integer, Long> postSequenceNumbers = readSequenceNumbersById(session, tableName);
            assertEquals(postSequenceNumbers.size(), 2);
            assertEquals((long) postSequenceNumbers.get(1), preSeq1);
            assertEquals((long) postSequenceNumbers.get(2), preSeq2);

            int lineageFieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
            assertTrue(committedFile.lowerBounds() != null
                            && committedFile.lowerBounds().containsKey(lineageFieldId),
                    "compaction file is missing lineage column lower bound stats");

            assertIdsForSequenceNumberPredicate(session, tableName, "<= " + preSeq1, ImmutableList.of(1));
            assertIdsForSequenceNumberPredicate(session, tableName, "<= " + preSeq2, ImmutableList.of(1, 2));
            assertIdsForSequenceNumberPredicate(session, tableName, "< " + preSeq1, ImmutableList.of());
            assertIdsForSequenceNumberPredicate(session, tableName, "> " + preSeq2, ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testV2TableLineagePredicates(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_pushdown_v2";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "2");
            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");

            assertIdsForSequenceNumberPredicate(session, tableName, "<= 100", ImmutableList.of());
            assertIdsForSequenceNumberPredicate(session, tableName, "> 0", ImmutableList.of());
            assertIdsForSequenceNumberPredicate(session, tableName, "IS NOT NULL", ImmutableList.of());
            assertIdsForSequenceNumberPredicate(session, tableName, "IS NULL", ImmutableList.of(1, 2));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testPredicateActuallyPrunesSplits(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_pushdown_split_count";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            appendIdAndValue(table, 3, "three");

            long minSeq = readSequenceNumbersById(session, tableName).get(1);

            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            String pruned = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" < " + minSeq;
            assertIdsForQuery(session, pruned, ImmutableList.of());
            int splitsPruned = completedSplitsFor(session, pruned);

            assertTrue(splitsAll > splitsPruned,
                    "expected predicate to prune splits but unrestricted=" + splitsAll
                            + " pruned=" + splitsPruned);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testDisjointOrRangesPruneMiddleFile(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_disjoint_or";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            appendIdAndValue(table, 3, "three");

            Map<Integer, Long> sequenceNumbers = readSequenceNumbersById(session, tableName);
            long seq1 = sequenceNumbers.get(1);
            long seq2 = sequenceNumbers.get(2);
            long seq3 = sequenceNumbers.get(3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            String disjoint = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" <= " + seq1
                    + " OR \"_last_updated_sequence_number\" >= " + seq3;

            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            int splitsDisjoint = completedSplitsFor(session, disjoint);
            assertTrue(splitsAll > splitsDisjoint,
                    "expected disjoint OR to prune middle file but unrestricted=" + splitsAll
                            + " disjoint=" + splitsDisjoint);

            assertIdsForQuery(session, disjoint, ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @DataProvider(name = "boundlessLineageMetricsMode")
    public Object[][] boundlessLineageMetricsModeProvider()
    {
        return withPushdownFilterEnabled("counts", "none");
    }

    @DataProvider(name = "fileFormat")
    public Object[][] fileFormatProvider()
    {
        return withPushdownFilterEnabled(FileFormat.PARQUET, FileFormat.ORC);
    }

    /**
     * Pairs each of {@code values} with each {@code pushdownFilterEnabled} value that
     * {@link #pushdownFilterEnabledProvider} returns.
     */
    private Object[][] withPushdownFilterEnabled(Object... values)
    {
        ImmutableList.Builder<Object[]> rows = ImmutableList.builder();
        for (Object value : values) {
            for (Object[] pushdownFilterEnabled : pushdownFilterEnabledProvider()) {
                rows.add(new Object[] {value, pushdownFilterEnabled[0]});
            }
        }
        return rows.build().toArray(new Object[0][]);
    }

    /**
     * A compacted file whose lineage columns have no bounds, only counts or no metrics at all,
     * stores values the coordinator cannot see, so it must not be pruned by its own, newer data
     * sequence number.
     */
    @Test(dataProvider = "boundlessLineageMetricsMode")
    public void testCompactedFileWithoutLineageBoundsIsNotMisPruned(String lineageMetricsMode, boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_metrics_" + lineageMetricsMode;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            Map<Integer, Long> sequenceNumbers = readSequenceNumbersById(session, tableName);
            long preCompactionSequenceNumber1 = sequenceNumbers.get(1);
            long preCompactionSequenceNumber2 = sequenceNumbers.get(2);
            MetricsConfig boundlessLineage = MetricsConfig.fromProperties(ImmutableMap.of(
                    "write.metadata.metrics.column." + MetadataColumns.ROW_ID.name(), lineageMetricsMode,
                    "write.metadata.metrics.column." + MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), lineageMetricsMode));
            DataFile committed = compactPreservingLineage(session, table, tableName, boundlessLineage);

            int lineageFieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
            assertEquals(committed.valueCounts() != null && committed.valueCounts().containsKey(lineageFieldId), lineageMetricsMode.equals("counts"),
                    "compacted file should carry lineage value counts only in metrics mode counts");
            assertTrue(committed.lowerBounds() == null || !committed.lowerBounds().containsKey(lineageFieldId),
                    "compacted file should carry no lineage bounds");
            assertTrue(committed.dataSequenceNumber() > preCompactionSequenceNumber2, "the rewrite must commit at a newer sequence number");

            assertIdsForSequenceNumberPredicate(session, tableName, "= " + preCompactionSequenceNumber1, ImmutableList.of(1));
            assertIdsForSequenceNumberPredicate(session, tableName, "= " + preCompactionSequenceNumber2, ImmutableList.of(2));
            assertIdsForSequenceNumberPredicate(session, tableName, "<= " + preCompactionSequenceNumber2, ImmutableList.of(1, 2));
            assertIdsForSequenceNumberPredicate(session, tableName, "= " + committed.dataSequenceNumber(), ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * A file without column metrics that an APPEND snapshot added is pruned by its data sequence
     * number, since an append stores no lineage values. The predicate admits values below the second
     * file's sequence number, so only the APPEND snapshot lets that file be pruned.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testAppendedFileWithoutMetricsIsPruned(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_append_without_metrics";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, TEST_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3", DEFAULT_WRITE_METRICS_MODE, "none"));
            appendIdAndValue(table, 1, "one");
            appendIdAndValue(table, 2, "two");
            assertNoColumnMetrics(table);
            long sequenceNumber1 = readSequenceNumbersById(session, tableName).get(1);

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + sequenceNumber1;
            assertIdsForQuery(session, selective, ImmutableList.of(1));
            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            int splitsSelective = completedSplitsFor(session, selective);
            assertTrue(splitsAll > splitsSelective,
                    "expected the second appended file to be pruned but unrestricted=" + splitsAll + " selective=" + splitsSelective);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * No row's value exceeds its file's sequence number, so a predicate above it prunes a file without
     * lineage metrics even after the APPEND snapshot that added the file is expired.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testFileFromExpiredAppendIsPrunedAboveItsSequenceNumber(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_expired_append_pruned";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, TEST_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3", DEFAULT_WRITE_METRICS_MODE, "none"));
            appendIdAndValue(table, 1, "one");
            long firstAppendSnapshotId = table.currentSnapshot().snapshotId();
            appendIdAndValue(table, 2, "two");
            assertNoColumnMetrics(table);
            long sequenceNumber1 = readSequenceNumbersById(session, tableName).get(1);
            table.expireSnapshots().expireSnapshotId(firstAppendSnapshotId).commit();
            table.refresh();

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" > " + sequenceNumber1;
            assertIdsForQuery(session, selective, ImmutableList.of(2));
            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            int splitsSelective = completedSplitsFor(session, selective);
            assertTrue(splitsAll > splitsSelective,
                    "expected the file from the expired append to be pruned but unrestricted=" + splitsAll + " selective=" + splitsSelective);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testLineagePredicateWithDeletedRow(boolean pushdownFilterEnabled)
            throws Exception
    {
        // AUTOMATIC join distribution computes stats for the equality delete scan, which plans its
        // delete files with the data scan's predicate, lineage domains included.
        Session session = automaticJoinSession(sessionWithPushdown(pushdownFilterEnabled));
        String tableName = "test_lineage_deleted_row";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            Schema schema = table.schema();
            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"),
                    GenericRecord.create(schema).copy("id", 3, "value", "three"));
            table.refresh();
            DataFile file = getOnlyElement(dataFiles(table));
            long firstRowId = file.firstRowId();
            writeEqualityDeleteOnId(table, 2);

            Map<Integer, Long> rowIds = readRowIdsById(session, tableName);
            assertEquals(rowIds.size(), 2);
            assertEquals((long) rowIds.get(1), firstRowId);
            assertEquals((long) rowIds.get(3), firstRowId + 2);

            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + (firstRowId + 2), ImmutableList.of(3));
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + (firstRowId + 1), ImmutableList.of());
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + file.dataSequenceNumber(), ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testTableStatisticsWithLineagePredicate(boolean pushdownFilterEnabled)
            throws Exception
    {
        // Statistics for the data scan and for the equality delete scan are computed with the
        // layout's predicate, lineage domains included, and Iceberg cannot bind lineage columns
        // when it plans either scan. AUTOMATIC join distribution makes the planner compute them.
        // SHOW STATS cannot cover this: a lineage predicate is also filtered per row, and SHOW
        // STATS rejects predicates the connector does not enforce.
        Session session = automaticJoinSession(sessionWithPushdown(pushdownFilterEnabled));
        String tableName = "test_table_statistics_lineage_predicate";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            Schema schema = table.schema();
            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"));
            table.refresh();
            DataFile file = getOnlyElement(dataFiles(table));
            List<String> predicates = ImmutableList.of(
                    "\"_row_id\" = " + file.firstRowId(),
                    "\"_last_updated_sequence_number\" = " + file.dataSequenceNumber(),
                    "\"_row_id\" = " + file.firstRowId() + " AND \"_last_updated_sequence_number\" = " + file.dataSequenceNumber());

            assertExplainWithPredicates(session, tableName, predicates);

            // The equality delete makes the plan join the data scan with an equality delete scan.
            writeEqualityDeleteOnId(table, 2);
            assertExplainWithPredicates(session, tableName, predicates);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    private void assertExplainWithPredicates(Session session, String tableName, List<String> predicates)
    {
        for (String predicate : predicates) {
            assertQuerySucceeds(session, "EXPLAIN SELECT * FROM " + tableName + " WHERE " + predicate);
        }
    }

    /**
     * Row group and stripe statistics for a lineage column cover only the stored values, so a
     * predicate matching an inherited value must not skip them.
     */
    @Test(dataProvider = "fileFormat")
    public void testRowIdPredicatesOnMixedStoredAndInheritedFile(FileFormat fileFormat, boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_mixed_file";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            MixedLineageTuple mixed = createMixedLineageTable(session, catalog, tableId, fileFormat);

            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + mixed.inheritedRowId, ImmutableList.of(2));
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + mixed.storedRowId, ImmutableList.of(1));
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" >= " + mixed.inheritedRowId, ImmutableList.of(2));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testIsNullOnV3Table(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String inheritedTableName = "test_lineage_is_null_inherited";
        String mixedTableName = "test_lineage_is_null_mixed";
        Catalog catalog = loadCatalog();
        TableIdentifier inheritedTableId = TableIdentifier.of(TEST_SCHEMA, inheritedTableName);
        TableIdentifier mixedTableId = TableIdentifier.of(TEST_SCHEMA, mixedTableName);
        try {
            Table inherited = createTestTable(catalog, inheritedTableId, "3");
            appendIdAndValue(inherited, 1, "one");
            appendIdAndValue(inherited, 2, "two");
            createMixedLineageTable(session, catalog, mixedTableId, FileFormat.PARQUET);

            for (String tableName : ImmutableList.of(inheritedTableName, mixedTableName)) {
                for (String column : ImmutableList.of("_row_id", "_last_updated_sequence_number")) {
                    assertIdsForPredicate(session, tableName, column, "IS NULL", ImmutableList.of());
                    assertIdsForPredicate(session, tableName, column, "IS NOT NULL", ImmutableList.of(1, 2));
                }
            }
        }
        finally {
            dropTableQuietly(catalog, inheritedTableId);
            dropTableQuietly(catalog, mixedTableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testMixedFileStillPrunedWhenNothingMatches(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_mixed_file_pruned";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            MixedLineageTuple mixed = createMixedLineageTable(session, catalog, tableId, FileFormat.PARQUET);
            Table table = catalog.loadTable(tableId);
            appendIdAndValue(table, 3, "three");
            long sequenceNumber3 = readSequenceNumbersById(session, tableName).get(3);
            assertTrue(sequenceNumber3 > mixed.inheritedSequenceNumber);

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + sequenceNumber3;
            assertIdsForQuery(session, selective, ImmutableList.of(3));
            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            int splitsSelective = completedSplitsFor(session, selective);
            assertTrue(splitsAll > splitsSelective,
                    "expected the mixed file to be pruned but unrestricted=" + splitsAll + " selective=" + splitsSelective);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    private void assertIdsForSequenceNumberPredicate(Session session, String tableName, String predicate, List<Integer> expectedIds)
    {
        assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", predicate, expectedIds);
    }

    private int completedSplitsFor(Session session, String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(session, sql);
        QueryStats stats = runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId())
                .getQueryStats();
        return stats.getCompletedSplits();
    }

    private static void assertNoColumnMetrics(Table table)
            throws Exception
    {
        for (DataFile file : dataFiles(table)) {
            assertTrue(file.valueCounts() == null || file.valueCounts().isEmpty(), "file should carry no column metrics: " + file.location());
        }
    }

    private static Session automaticJoinSession(Session session)
    {
        return Session.builder(session)
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "AUTOMATIC")
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .build();
    }

    /**
     * V3 requires deletion vectors for position deletes, which the Java worker does not read, so
     * the tests write equality deletes. A v3 reader is expected to read v2 equality delete files
     * for backwards compatibility.
     */
    private static void writeEqualityDeleteOnId(Table table, int id)
            throws Exception
    {
        Schema deleteSchema = table.schema().select("id");
        org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(table.location(), "data/delete-" + UUID.randomUUID() + ".parquet");
        EqualityDeleteWriter<Record> writer = Parquet.writeDeletes(HadoopOutputFile.fromPath(path, new Configuration()))
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .rowSchema(deleteSchema)
                .withSpec(table.spec())
                .equalityFieldIds(deleteSchema.findField("id").fieldId())
                .buildEqualityWriter();
        try (Closeable ignored = writer) {
            writer.write(GenericRecord.create(deleteSchema).copy("id", id));
        }
        table.newRowDelta().addDeletes(writer.toDeleteFile()).commit();
        table.refresh();
    }

    /**
     * Commits ids 1 and 2 in separate appends, then rewrites both into one file in which id 1
     * stores its original lineage and id 2 stores none, so id 2 inherits the new file's
     * {@code firstRowId + 1} and data sequence number. The rewritten file is written in
     * {@code fileFormat}.
     */
    private MixedLineageTuple createMixedLineageTable(Session session, Catalog catalog, TableIdentifier tableId, FileFormat fileFormat)
            throws Exception
    {
        String tableName = tableId.name();
        Table table = createTestTable(catalog, tableId, "3");
        appendIdAndValue(table, 1, "one");
        appendIdAndValue(table, 2, "two");
        long storedRowId = readRowIdsById(session, tableName).get(1);
        long storedSequenceNumber = readSequenceNumbersById(session, tableName).get(1);

        Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
        Record stored = lineageRecord(lineageSchema, 1, "one", storedRowId, storedSequenceNumber);
        Record inherited = lineageRecord(lineageSchema, 2, "two-updated", null, null);
        OverwriteFiles overwrite = table.newOverwrite();
        for (DataFile file : dataFiles(table)) {
            overwrite.deleteFile(file);
        }
        overwrite.addFile(writeFileInFormat(table, lineageSchema, fileFormat, stored, inherited)).commit();
        table.refresh();

        DataFile file = getOnlyElement(dataFiles(table));
        MixedLineageTuple mixed = new MixedLineageTuple(storedRowId, storedSequenceNumber, file.firstRowId() + 1, file.dataSequenceNumber());
        assertTrue(mixed.inheritedSequenceNumber > mixed.storedSequenceNumber, "the rewrite must commit at a newer sequence number");

        Map<Integer, Long> rowIds = readRowIdsById(session, tableName);
        assertEquals((long) rowIds.get(1), mixed.storedRowId);
        assertEquals((long) rowIds.get(2), mixed.inheritedRowId);
        Map<Integer, Long> sequenceNumbers = readSequenceNumbersById(session, tableName);
        assertEquals((long) sequenceNumbers.get(1), mixed.storedSequenceNumber);
        assertEquals((long) sequenceNumbers.get(2), mixed.inheritedSequenceNumber);
        return mixed;
    }

    private DataFile writeFileInFormat(Table table, Schema writeSchema, FileFormat fileFormat, Record... records)
            throws Exception
    {
        if (fileFormat == FileFormat.PARQUET) {
            return writeFile(table, writeSchema, records);
        }
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(table.location(), "data/" + fileFormat.addExtension("data-" + UUID.randomUUID()));
        return writeOrcFile(table, writeSchema, filePath, records);
    }

    /**
     * Writes {@code records} with the Apache ORC writer bundled in hive-apache, since iceberg-orc
     * needs a different build of the same ORC classes. Each column carries its Iceberg field id,
     * as Iceberg's ORC writer does. The data file has no column metrics.
     */
    private static DataFile writeOrcFile(Table table, Schema writeSchema, org.apache.hadoop.fs.Path filePath, Record... records)
            throws Exception
    {
        TypeDescription orcSchema = TypeDescription.createStruct();
        for (Types.NestedField field : writeSchema.columns()) {
            TypeDescription type;
            switch (field.type().typeId()) {
                case INTEGER:
                    type = TypeDescription.createInt();
                    break;
                case LONG:
                    type = TypeDescription.createLong();
                    break;
                case STRING:
                    type = TypeDescription.createString();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported type: " + field.type());
            }
            orcSchema.addField(field.name(), type.setAttribute(ORC_ICEBERG_ID_KEY, String.valueOf(field.fieldId())));
        }
        Configuration configuration = new Configuration();
        VectorizedRowBatch batch = orcSchema.createRowBatch();
        for (Record record : records) {
            int row = batch.size++;
            for (int column = 0; column < batch.cols.length; column++) {
                ColumnVector vector = batch.cols[column];
                Object value = record.get(column);
                if (value == null) {
                    vector.noNulls = false;
                    vector.isNull[row] = true;
                }
                else if (vector instanceof BytesColumnVector) {
                    ((BytesColumnVector) vector).setVal(row, ((String) value).getBytes(UTF_8));
                }
                else {
                    ((LongColumnVector) vector).vector[row] = ((Number) value).longValue();
                }
            }
        }
        try (Writer writer = OrcFile.createWriter(filePath, OrcFile.writerOptions(configuration).setSchema(orcSchema))) {
            writer.addRowBatch(batch);
        }
        return DataFiles.builder(table.spec())
                .withInputFile(HadoopInputFile.fromPath(filePath, configuration))
                .withFormat(org.apache.iceberg.FileFormat.ORC)
                .withRecordCount(records.length)
                .build();
    }

    private static class MixedLineageTuple
    {
        private final long storedRowId;
        private final long storedSequenceNumber;
        private final long inheritedRowId;
        private final long inheritedSequenceNumber;

        private MixedLineageTuple(long storedRowId, long storedSequenceNumber, long inheritedRowId, long inheritedSequenceNumber)
        {
            this.storedRowId = storedRowId;
            this.storedSequenceNumber = storedSequenceNumber;
            this.inheritedRowId = inheritedRowId;
            this.inheritedSequenceNumber = inheritedSequenceNumber;
        }
    }
}
