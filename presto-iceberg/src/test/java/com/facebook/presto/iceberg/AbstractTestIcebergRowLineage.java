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

import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
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

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Helpers shared by the Iceberg row lineage ({@code _row_id} /
 * {@code _last_updated_sequence_number}) tests. This class holds no tests of its own.
 * <p>
 * Tables are created and written through the Iceberg API rather than through Presto because row
 * lineage is assigned by the writer, and the cases these helpers support -- multiple commits, a V2
 * to V3 upgrade, compaction, and physically materialized lineage columns -- cannot be produced
 * with Presto DML alone. The expected lineage values are then derived from the Iceberg metadata
 * ({@code firstRowId + position} and {@code dataSequenceNumber}) so that Presto's answers are
 * checked against the file layout rather than against themselves.
 */
public abstract class AbstractTestIcebergRowLineage
        extends AbstractTestQueryFramework
{
    protected static final String TEST_SCHEMA = "tpch";

    protected static final Schema TEST_TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));

    /**
     * Warehouse directory the Presto catalog under test reads from. Subclasses point the Iceberg
     * API at the same directory so both see one set of files.
     */
    protected abstract File getCatalogDirectory();

    /**
     * Cross-checks {@code sql} against a reference engine. No-op by default, because a subclass
     * without an expected query runner over this same warehouse has nothing meaningful to compare
     * against. Subclasses that do configure one override this to compare the two engines.
     */
    protected void assertMatchesReferenceEngine(String sql)
    {
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
        Path filePath = new Path(table.location(), "data/data-" + UUID.randomUUID() + ".parquet");
        DataWriter<Record> writer = Parquet.writeData(HadoopOutputFile.fromPath(filePath, new Configuration()))
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
        assertMatchesReferenceEngine(sql);
        assertRowLineagePairs(computeActual(sql), expectedPairs);
    }

    protected void assertIdsForPredicate(String tableName, String predicate, List<Integer> expectedIds)
    {
        String sql = "SELECT id FROM " + tableName +
                " WHERE \"_last_updated_sequence_number\" " + predicate +
                " ORDER BY id";
        assertMatchesReferenceEngine(sql);
        assertEquals(idsOf(computeActual(sql)), expectedIds, "rows for predicate \"" + predicate + "\"");
    }

    protected List<long[]> readIdAndSequenceNumber(String tableName)
    {
        String sql = "SELECT id, \"_last_updated_sequence_number\" FROM " + tableName + " ORDER BY id";
        assertMatchesReferenceEngine(sql);
        MaterializedResult result = computeActual(sql);
        List<long[]> rows = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            rows.add(new long[] {(Integer) row.getField(0), (Long) row.getField(1)});
        }
        return rows;
    }

    protected static long sequenceNumberForId(List<long[]> rows, int id)
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
