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
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
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
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.Closeable;
import java.io.File;
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
 * Helpers shared by the Iceberg row lineage ({@code _row_id} /
 * {@code _last_updated_sequence_number}) tests. This class holds no tests of its own.
 * <p>
 * The query runner under test uses native workers; the expected query runner uses Java workers.
 * Both resolve to the same data directory and HADOOP catalog, so {@code assertQuery*} compares the
 * two workers over one set of files. The Java worker rejects Iceberg filter pushdown, so it is the
 * pushdown-disabled reference for native queries run with pushdown enabled.
 * <p>
 * Tables are created and written through the Iceberg API rather than through Presto because row
 * lineage is assigned by the writer, and the cases these helpers support -- multiple commits, a V2
 * to V3 upgrade, compaction, and physically materialized lineage columns -- cannot be produced
 * with Presto DML alone. Expected values come from the Iceberg metadata or from the values
 * written, so a bug shared by both workers still fails the test.
 */
public abstract class AbstractTestIcebergRowLineage
        extends AbstractTestQueryFramework
{
    protected static final String TEST_SCHEMA = "tpch";

    protected static final Schema TEST_TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));

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

    protected File getCatalogDirectory()
    {
        return IcebergQueryRunner.getIcebergDataDirectoryPath(
                        getDistributedQueryRunner().getCoordinator().getDataDirectory(),
                        CatalogType.HADOOP.name(),
                        new IcebergConfig().getFileFormat(),
                        true)
                .toFile();
    }

    protected Session pushdownFilterSession()
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, "true")
                .build();
    }

    /**
     * Runs {@code sql} on the native workers with filter pushdown enabled and on the Java workers
     * with it disabled, and asserts both return the same rows.
     */
    protected void assertMatchesJavaWorkerWithPushdown(String sql)
    {
        assertQuery(pushdownFilterSession(), sql, getSession(), sql);
    }

    protected Catalog loadCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(),
                ICEBERG_CATALOG,
                ImmutableMap.of("warehouse", getCatalogDirectory().toURI().toString()),
                new Configuration());
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

    protected static Table createTestTable(Catalog catalog, TableIdentifier tableId, String formatVersion)
    {
        return catalog.createTable(
                tableId,
                TEST_TABLE_SCHEMA,
                PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", formatVersion));
    }

    /**
     * Writes {@code records} to a new Parquet data file under the table's data directory and
     * returns the resulting {@link DataFile} without committing it, so callers can choose between
     * an append and a rewrite.
     * <p>
     * {@code writeSchema} is explicit rather than always {@code table.schema()} so that callers can
     * write physical {@code _row_id} / {@code _last_updated_sequence_number} values via
     * {@link org.apache.iceberg.MetadataColumns#schemaWithRowLineage}.
     */
    protected static DataFile writeFile(Table table, Schema writeSchema, Record... records)
            throws Exception
    {
        return writeFile(table, writeSchema, MetricsConfig.forTable(table), records);
    }

    protected static DataFile writeFile(Table table, Schema writeSchema, MetricsConfig metricsConfig, Record... records)
            throws Exception
    {
        Path filePath = new Path(table.location(), "data/data-" + UUID.randomUUID() + ".parquet");
        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(filePath, new Configuration()))
                .schema(writeSchema)
                .withSpec(table.spec())
                .createWriterFunc(GenericParquetWriter::create)
                .metricsConfig(metricsConfig)
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
        return writer.toDataFile();
    }

    protected static void writeRecords(Table table, Record... records)
            throws Exception
    {
        writeRecordsWithSchema(table, table.schema(), records);
    }

    protected static void writeRecordsWithSchema(Table table, Schema writeSchema, Record... records)
            throws Exception
    {
        table.newAppend().appendFile(writeFile(table, writeSchema, records)).commit();
    }

    /**
     * Replaces every data file in the table with one file holding {@code rows}, as a
     * copy-on-write UPDATE does. Rows that set {@code _row_id} / {@code _last_updated_sequence_number}
     * keep those stored values; rows that leave them null inherit {@code firstRowId + position} and
     * the new file's data sequence number, so the file mixes stored and inherited lineage.
     */
    protected static void writeMixedLineageFile(Table table, Schema lineageSchema, Record... rows)
            throws Exception
    {
        OverwriteFiles overwrite = table.newOverwrite();
        for (DataFile file : dataFiles(table)) {
            overwrite.deleteFile(file);
        }
        overwrite.addFile(writeFile(table, lineageSchema, rows)).commit();
        table.refresh();
    }

    /**
     * Commits an equality delete on {@code id}. V3 requires deletion vectors for position deletes,
     * and the Java worker does not read deletion vectors, so equality deletes are the delete form
     * both workers can read.
     */
    protected static void writeEqualityDeleteOnId(Table table, int id)
            throws Exception
    {
        Schema deleteSchema = table.schema().select("id");
        Path path = new Path(table.location(), "data/delete-" + UUID.randomUUID() + ".parquet");
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

    protected static List<DataFile> dataFiles(Table table)
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

    /**
     * Appends a single row in its own commit, so each row lands in its own data file with its own
     * data sequence number.
     */
    protected static void appendOneRow(Table table, int id, String value)
            throws Exception
    {
        Record record = GenericRecord.create(table.schema());
        record.setField("id", id);
        record.setField("value", value);
        writeRecords(table, record);
        table.refresh();
    }

    /**
     * Derives the {@code (_row_id, _last_updated_sequence_number)} pairs the table should report,
     * from the Iceberg metadata alone: each row's id is its file's {@code firstRowId} plus its
     * position, and its sequence number is the file's {@code dataSequenceNumber}. Sorted by row id
     * to match a query ordered on {@code _row_id}.
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

    /**
     * Verifies an already-materialized {@code (_row_id, _last_updated_sequence_number)} result
     * against {@code expectedPairs}. Takes the result rather than running the query so callers that
     * have already executed it do not pay for a second execution.
     */
    protected static void assertRowLineagePairs(MaterializedResult result, List<long[]> expectedPairs)
    {
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
     * Indexes a {@code (id, _row_id, _last_updated_sequence_number)} result by id, asserting both
     * lineage values are present.
     */
    protected static Map<Integer, long[]> rowIdAndSeqById(MaterializedResult result)
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

    protected static List<Integer> idsOf(MaterializedResult result)
    {
        List<Integer> ids = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            ids.add((Integer) row.getField(0));
        }
        return ids;
    }

    protected void assertPrestoRowLineageMatchesExpected(String tableName, List<long[]> expectedPairs)
    {
        String sql = "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                " ORDER BY \"_row_id\"";
        assertQueryOrdered(sql);
        assertRowLineagePairs(computeActual(sql), expectedPairs);
    }

    protected void assertIdsForPredicate(String tableName, String predicate, List<Integer> expectedIds)
    {
        assertIdsForPredicate(tableName, "_last_updated_sequence_number", predicate, expectedIds);
    }

    /**
     * Asserts that {@code WHERE "column" predicate} returns {@code expectedIds} on the native
     * workers with filter pushdown disabled and enabled, and that both match the Java workers.
     * Only {@code id} is selected, so {@code column} is read for the filter alone.
     */
    protected void assertIdsForPredicate(String tableName, String column, String predicate, List<Integer> expectedIds)
    {
        String sql = "SELECT id FROM " + tableName +
                " WHERE \"" + column + "\" " + predicate +
                " ORDER BY id";
        assertIdsForQuery(sql, expectedIds);
    }

    /**
     * Asserts that {@code sql}, whose first column is {@code id}, returns {@code expectedIds} on the
     * native workers with filter pushdown disabled and enabled, and that both match the Java workers.
     */
    protected void assertIdsForQuery(String sql, List<Integer> expectedIds)
    {
        assertQueryOrdered(sql);
        assertMatchesJavaWorkerWithPushdown(sql);
        assertEquals(idsOf(computeActual(sql)), expectedIds, "rows for \"" + sql + "\"");
        assertEquals(idsOf(computeActual(pushdownFilterSession(), sql)), expectedIds,
                "rows with filter pushdown for \"" + sql + "\"");
    }

    protected List<long[]> readIdAndSequenceNumber(String tableName)
    {
        String sql = "SELECT id, \"_last_updated_sequence_number\" FROM " + tableName + " ORDER BY id";
        assertQueryOrdered(sql);
        MaterializedResult result = computeActual(sql);
        List<long[]> rows = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            rows.add(new long[] {(Integer) row.getField(0), (Long) row.getField(1)});
        }
        return rows;
    }

    protected List<long[]> readIdAndRowId(String tableName)
    {
        String sql = "SELECT id, \"_row_id\" FROM " + tableName + " ORDER BY id";
        assertQueryOrdered(sql);
        List<long[]> rows = new ArrayList<>();
        for (MaterializedRow row : computeActual(sql).getMaterializedRows()) {
            rows.add(new long[] {(Integer) row.getField(0), (Long) row.getField(1)});
        }
        return rows;
    }

    protected static long valueForId(List<long[]> rows, int id)
    {
        for (long[] row : rows) {
            if (row[0] == id) {
                return row[1];
            }
        }
        throw new AssertionError("id not found: " + id);
    }

    protected int completedSplitsFor(String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(getSession(), sql);
        QueryStats stats = runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId())
                .getQueryStats();
        return stats.getCompletedSplits();
    }
}
