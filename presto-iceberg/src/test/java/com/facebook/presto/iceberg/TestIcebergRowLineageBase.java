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

import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public abstract class TestIcebergRowLineageBase
        extends AbstractTestQueryFramework
{
    protected static final String TEST_SCHEMA = "tpch";

    protected abstract File getCatalogDirectory();

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

            assertPrestoRowLineageMatchesExpected(tableName, expectedPairs);

            long distinctRowIds = (Long) computeActual(
                    "SELECT count(DISTINCT \"_row_id\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctRowIds, 2L, "Row IDs must be unique across all rows");

            long distinctSeqNums = (Long) computeActual(
                    "SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctSeqNums, 2L, "Sequence numbers should differ between commits");

            Long seqForFirst = (Long) computeActual(
                    "SELECT \"_last_updated_sequence_number\" FROM " + tableName + " WHERE id = 1").getOnlyValue();
            Long seqForSecond = (Long) computeActual(
                    "SELECT \"_last_updated_sequence_number\" FROM " + tableName + " WHERE id = 2").getOnlyValue();
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

            assertPrestoRowLineageMatchesExpected(tableName, expectedPairs);

            long sharedSeqNum = expectedPairs.get(0)[1];
            for (long[] pair : expectedPairs) {
                assertEquals(pair[1], sharedSeqNum,
                        "All rows in a single commit should have the same sequence number");
            }

            long distinctRowIds = (Long) computeActual(
                    "SELECT count(DISTINCT \"_row_id\") FROM " + tableName).getOnlyValue();
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
            assertEquals(computeActual("SELECT \"_row_id\", * FROM " + tableName).getRowCount(), 3);
            assertEquals(
                    computeActual("SELECT count(*) FROM " + tableName + " WHERE \"_row_id\" IS NOT NULL").getOnlyValue(),
                    0L, "_row_id should be null for all rows in a V2 table");
            assertEquals(
                    computeActual("SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NOT NULL").getOnlyValue(),
                    0L, "_last_updated_sequence_number should be null for all rows in a V2 table");

            table.refresh();
            table.updateProperties().set("format-version", "3").commit();
            table.refresh();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 4, "value", "four"),
                    GenericRecord.create(schema).copy("id", 5, "value", "five"));
            table.refresh();

            assertEquals(computeActual("SELECT count(*) FROM " + tableName +
                            " WHERE \"_row_id\" IS NULL").getOnlyValue(), 0L,
                    "All rows should have non-null _row_id after V3 upgrade");
            assertEquals(computeActual("SELECT count(*) FROM " + tableName +
                            " WHERE \"_last_updated_sequence_number\" IS NULL").getOnlyValue(), 0L,
                    "All rows should have non-null _last_updated_sequence_number after V3 upgrade");

            long distinctRowIds = (Long) computeActual(
                    "SELECT count(DISTINCT \"_row_id\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctRowIds, 5L, "Row IDs must be unique across all 5 rows after upgrade");

            table.refresh();
            List<long[]> allExpectedPairs = buildExpectedPairs(table,
                    "All files should have firstRowId set after V3 upgrade");
            assertPrestoRowLineageMatchesExpected(tableName, allExpectedPairs);
        }
        finally {
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
    }

    protected void assertPrestoRowLineageMatchesExpected(String tableName, List<long[]> expectedPairs)
    {
        MaterializedResult result = computeActual(
                "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                        " ORDER BY \"_row_id\"");
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
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "value", Types.StringType.get()));
        return catalog.createTable(
                tableId,
                schema,
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", formatVersion));
    }

    protected void writeRecords(Table table, Record... records)
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
}
