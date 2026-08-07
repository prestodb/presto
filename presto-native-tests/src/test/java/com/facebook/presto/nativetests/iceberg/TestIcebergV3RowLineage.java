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
package com.facebook.presto.nativetests.iceberg;

import com.facebook.presto.Session;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Iceberg V3 row lineage (_row_id / _last_updated_sequence_number) tests for the native engine.
 * <p>
 * Every lineage query is checked twice: once with {@code assertQuery}, which runs it on both the
 * native engine under test and the Java engine (the expected query runner) over the same warehouse
 * and compares the results, and once against the values derived from the Iceberg metadata itself.
 * <p>
 * The tables are created and written through the Iceberg API rather than through Presto because
 * row lineage is assigned by the writer, and these cases -- multiple commits, a V2 to V3 upgrade,
 * and physically materialized lineage columns -- cannot be produced with Presto DML alone.
 */
public class TestIcebergV3RowLineage
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void testV3TableRowLineageMatchesIcebergMetadata()
            throws Exception
    {
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

            assertRowLineageMatchesIcebergMetadata(tableName, expectedPairs);

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(distinctRowIdsSql);
            long distinctRowIds = (Long) computeScalar(distinctRowIdsSql);
            assertEquals(distinctRowIds, 2L, "Row IDs must be unique across all rows");

            String distinctSeqNumsSql = "SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + tableName;
            assertQuery(distinctSeqNumsSql);
            long distinctSeqNums = (Long) computeScalar(distinctSeqNumsSql);
            assertEquals(distinctSeqNums, 2L, "Sequence numbers should differ between commits");

            String seqForFirstSql = "SELECT \"_last_updated_sequence_number\" FROM " + tableName + " WHERE id = 1";
            String seqForSecondSql = "SELECT \"_last_updated_sequence_number\" FROM " + tableName + " WHERE id = 2";
            assertQuery(seqForFirstSql);
            assertQuery(seqForSecondSql);
            Long seqForFirst = (Long) computeScalar(seqForFirstSql);
            Long seqForSecond = (Long) computeScalar(seqForSecondSql);
            assertTrue(seqForFirst < seqForSecond,
                    "_last_updated_sequence_number should be smaller for earlier commits");
        }
        finally {
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testV3TableRowLineageWithMultipleRowsPerCommit()
            throws Exception
    {
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

            assertRowLineageMatchesIcebergMetadata(tableName, expectedPairs);

            long sharedSeqNum = expectedPairs.get(0)[1];
            for (long[] pair : expectedPairs) {
                assertEquals(pair[1], sharedSeqNum,
                        "All rows in a single commit should have the same sequence number");
            }

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(distinctRowIdsSql);
            long distinctRowIds = (Long) computeScalar(distinctRowIdsSql);
            assertEquals(distinctRowIds, 3L, "Row IDs must be unique across all rows");
        }
        finally {
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testRowLineageBackfilledOnV2ToV3Upgrade()
            throws Exception
    {
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
            assertQuery(allRowsSql);
            assertEquals(computeActual(allRowsSql).getRowCount(), 3);

            String nonNullRowIdsSql = "SELECT count(*) FROM " + tableName + " WHERE \"_row_id\" IS NOT NULL";
            assertQuery(nonNullRowIdsSql);
            assertEquals(computeScalar(nonNullRowIdsSql), 0L,
                    "_row_id should be null for all rows in a V2 table");

            String nonNullSeqNumsSql = "SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NOT NULL";
            assertQuery(nonNullSeqNumsSql);
            assertEquals(computeScalar(nonNullSeqNumsSql), 0L,
                    "_last_updated_sequence_number should be null for all rows in a V2 table");

            table.refresh();
            table.updateProperties().set("format-version", "3").commit();
            table.refresh();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 4, "value", "four"),
                    GenericRecord.create(schema).copy("id", 5, "value", "five"));
            table.refresh();

            String nullRowIdsSql = "SELECT count(*) FROM " + tableName + " WHERE \"_row_id\" IS NULL";
            assertQuery(nullRowIdsSql);
            assertEquals(computeScalar(nullRowIdsSql), 0L,
                    "All rows should have non-null _row_id after V3 upgrade");

            String nullSeqNumsSql = "SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NULL";
            assertQuery(nullSeqNumsSql);
            assertEquals(computeScalar(nullSeqNumsSql), 0L,
                    "All rows should have non-null _last_updated_sequence_number after V3 upgrade");

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(distinctRowIdsSql);
            long distinctRowIds = (Long) computeScalar(distinctRowIdsSql);
            assertEquals(distinctRowIds, 5L, "Row IDs must be unique across all 5 rows after upgrade");

            table.refresh();
            List<long[]> allExpectedPairs = buildExpectedPairs(table,
                    "All files should have firstRowId set after V3 upgrade");
            assertRowLineageMatchesIcebergMetadata(tableName, allExpectedPairs);
        }
        finally {
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Regression test: a row whose _row_id/_last_updated_sequence_number were explicitly
     * written into the physical data file (as an external writer like Spark does for a
     * row-preserving UPDATE/MERGE, to override the firstRowId+position fallback) must report
     * the same values whether or not the query also predicates/orders on that column. Presto's
     * native connector previously only taught the reader about these columns' physical field
     * IDs when they appeared in a filter predicate, so a plain projection query silently fell
     * back to firstRowId+position (for _row_id) or the file's own dataSequenceNumber (for
     * _last_updated_sequence_number) instead of reading the real per-row value.
     */
    @Test
    public void testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueries()
            throws Exception
    {
        String tableName = "test_row_lineage_predicate_vs_projection";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            Schema schema = table.schema();

            // Pure insert: relies on the firstRowId + position fallback.
            writeRecords(table, GenericRecord.create(schema).copy("id", 1, "value", "one"));
            table.refresh();

            // Simulate a row-preserving external UPDATE/MERGE (e.g. real Spark MERGE INTO):
            // the new data file explicitly carries _row_id/_last_updated_sequence_number
            // values that must override the positional/file-level fallback.
            Schema lineageSchema = MetadataColumns.schemaWithRowLineage(schema);
            Record updatedRow = GenericRecord.create(lineageSchema);
            updatedRow.setField("id", 2);
            updatedRow.setField("value", "two-updated");
            updatedRow.setField(MetadataColumns.ROW_ID.name(), 42L);
            updatedRow.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), 99L);
            writeRecordsWithSchema(table, lineageSchema, updatedRow);
            table.refresh();

            String unfilteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                    " ORDER BY id";
            String filteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                    " WHERE \"_row_id\" IS NOT NULL ORDER BY id";
            assertQueryOrdered(unfilteredSql);
            assertQueryOrdered(filteredSql);

            MaterializedResult unfiltered = computeActual(unfilteredSql);
            MaterializedResult filtered = computeActual(filteredSql);

            assertEquals(unfiltered.getRowCount(), 2);
            Map<Integer, long[]> unfilteredById = rowIdAndSeqById(unfiltered);
            Map<Integer, long[]> filteredById = rowIdAndSeqById(filtered);
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
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Same regression as {@link #testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueries}, but
     * with pushdown_filter_enabled=true. With pushdown filter on, IcebergTableLayoutHandle is built
     * exclusively by IcebergFilterPushdown, which (before this fix) only ever forwarded the
     * requestedColumns of a prior layout instead of deriving them from the table scan's actual output
     * columns -- and there is no prior layout on the first pass, so requestedColumns silently resolved
     * to empty and the physical override was lost, even though the plain (pushdown filter disabled)
     * query above returned the correct value.
     */
    @Test
    public void testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueriesWithPushdownFilter()
            throws Exception
    {
        String tableName = "test_row_lineage_predicate_vs_projection_pushdown";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            Schema schema = table.schema();

            writeRecords(table, GenericRecord.create(schema).copy("id", 1, "value", "one"));
            table.refresh();

            Schema lineageSchema = MetadataColumns.schemaWithRowLineage(schema);
            Record updatedRow = GenericRecord.create(lineageSchema);
            updatedRow.setField("id", 2);
            updatedRow.setField("value", "two-updated");
            updatedRow.setField(MetadataColumns.ROW_ID.name(), 42L);
            updatedRow.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), 99L);
            writeRecordsWithSchema(table, lineageSchema, updatedRow);
            table.refresh();

            Session pushdownFilterSession = Session.builder(getSession())
                    .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                    .build();

            String unfilteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                    " ORDER BY id";
            // Cross-checked against the same (native) query runner with pushdown filter disabled
            // rather than against the Java expected query runner: filter pushdown is native-only,
            // and the Java Iceberg connector rejects it outright (IcebergPageSourceProvider throws
            // NOT_SUPPORTED). Enabling pushdown must not change the answer.
            assertQueryWithSameQueryRunner(pushdownFilterSession, unfilteredSql, getSession());

            MaterializedResult unfiltered = computeActual(pushdownFilterSession, unfilteredSql);

            assertEquals(unfiltered.getRowCount(), 2);
            Map<Integer, long[]> unfilteredById = rowIdAndSeqById(unfiltered);
            assertEquals(unfilteredById.get(2)[0], 42L,
                    "_row_id should reflect the file's explicit physical value under pushdown filter too, not firstRowId+position");
            assertEquals(unfilteredById.get(2)[1], 99L,
                    "_last_updated_sequence_number should reflect the file's explicit physical value under pushdown filter too");
        }
        finally {
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
    }

    /**
     * Asserts that the native engine and the Java engine return the same lineage rows, and that
     * those rows match the {@code firstRowId + position} / {@code dataSequenceNumber} pairs the
     * Iceberg metadata says the table should have.
     */
    private void assertRowLineageMatchesIcebergMetadata(String tableName, List<long[]> expectedPairs)
    {
        String sql = "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                " ORDER BY \"_row_id\"";
        assertQueryOrdered(sql);

        List<MaterializedRow> rows = computeActual(sql).getMaterializedRows();
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

    private static Map<Integer, long[]> rowIdAndSeqById(MaterializedResult result)
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

    private static List<long[]> buildExpectedPairs(Table table, String firstRowIdMessage)
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

    private static Table createTestTable(Catalog catalog, TableIdentifier tableId, String formatVersion)
    {
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "value", Types.StringType.get()));
        return catalog.createTable(
                tableId,
                schema,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", formatVersion));
    }

    private static void writeRecords(Table table, Record... records)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);
        Configuration conf = new Configuration();

        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(filePath, conf))
                .forTable(table)
                .createWriterFunc(GenericParquetWriter::create)
                .overwrite()
                .build();
        try {
            for (Record record : records) {
                writer.write(record);
            }
        }
        finally {
            writer.close();
        }

        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    /**
     * Like {@link #writeRecords}, but writes against an explicit schema rather than
     * {@code table.schema()} -- needed to write physical {@code _row_id}/
     * {@code _last_updated_sequence_number} values via {@link MetadataColumns#schemaWithRowLineage}.
     */
    private static void writeRecordsWithSchema(Table table, Schema writeSchema, Record... records)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);
        Configuration conf = new Configuration();

        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(filePath, conf))
                .schema(writeSchema)
                .withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create)
                .metricsConfig(MetricsConfig.forTable(table))
                .overwrite()
                .build();
        try {
            for (Record record : records) {
                writer.write(record);
            }
        }
        finally {
            writer.close();
        }

        table.newAppend().appendFile(writer.toDataFile()).commit();
    }

    private Catalog loadCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
                ImmutableMap.of("warehouse", getCatalogDirectory().toURI().toString()),
                new Configuration());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = IcebergQueryRunner.getIcebergDataDirectoryPath(
                dataDirectory, CatalogType.HADOOP.name(),
                new IcebergConfig().getFileFormat(), true);
        return catalogDirectory.toFile();
    }
}
