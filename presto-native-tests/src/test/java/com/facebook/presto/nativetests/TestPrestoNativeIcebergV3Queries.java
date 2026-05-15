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
package com.facebook.presto.nativetests;

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
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestPrestoNativeIcebergV3Queries
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    // Unique suffix per test-class instantiation.
    private static final String RUN_ID =
            UUID.randomUUID().toString().replace("-", "").substring(0, 8);

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
        String tableName = "test_native_row_lineage_" + RUN_ID;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);

        try {
            Schema schema = createTestSchema();
            Table table = createTestTable(catalog, tableId, "3");

            writeRecords(table, GenericRecord.create(schema).copy("id", 1, "value", "one"));
            table.refresh();
            writeRecords(table, GenericRecord.create(schema).copy("id", 2, "value", "two"));

            table.refresh();
            List<long[]> expectedPairs = buildExpectedPairs(table, "Iceberg should set firstRowId for V3 tables");

            assertQuery("SELECT \"_row_id\", \"_last_updated_sequence_number\", * FROM " + tableName);

            MaterializedResult prestoResult = computeActual(
                    "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                            " ORDER BY \"_row_id\"");
            List<MaterializedRow> prestoRows = prestoResult.getMaterializedRows();

            assertEquals(prestoRows.size(), expectedPairs.size(),
                    "Presto and Iceberg API should return the same number of rows");

            for (int i = 0; i < prestoRows.size(); i++) {
                Long prestoRowId = (Long) prestoRows.get(i).getField(0);
                Long prestoSeqNum = (Long) prestoRows.get(i).getField(1);

                assertNotNull(prestoRowId, "Presto _row_id should not be null for V3 table");
                assertNotNull(prestoSeqNum, "Presto _last_updated_sequence_number should not be null");

                assertEquals(prestoRowId.longValue(), expectedPairs.get(i)[0],
                        "Presto _row_id should match expected value from Iceberg metadata");
                assertEquals(prestoSeqNum.longValue(), expectedPairs.get(i)[1],
                        "Presto _last_updated_sequence_number should match Iceberg metadata");
            }

            long distinctRowIds = (Long) computeActual(
                    "SELECT count(DISTINCT \"_row_id\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctRowIds, 2L, "Row IDs must be unique across all rows");

            long distinctSeqNums = (Long) computeActual(
                    "SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctSeqNums, 2L, "Sequence numbers should differ between commits");

            Long seqForFirst = (Long) computeActual(
                    "SELECT \"_last_updated_sequence_number\" FROM " + tableName +
                            " WHERE id = 1").getOnlyValue();
            Long seqForSecond = (Long) computeActual(
                    "SELECT \"_last_updated_sequence_number\" FROM " + tableName +
                            " WHERE id = 2").getOnlyValue();
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
        String tableName = "test_native_row_lineage_multi_" + RUN_ID;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);

        try {
            Schema schema = createTestSchema();
            Table table = createTestTable(catalog, tableId, "3");

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"),
                    GenericRecord.create(schema).copy("id", 3, "value", "three"));

            table.refresh();
            List<long[]> expectedPairs = buildExpectedPairs(table, "firstRowId should be set for V3 tables");

            assertQuery("SELECT \"_row_id\", \"_last_updated_sequence_number\", * FROM " + tableName);

            MaterializedResult prestoResult = computeActual(
                    "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                            " ORDER BY \"_row_id\"");
            List<MaterializedRow> prestoRows = prestoResult.getMaterializedRows();
            assertEquals(prestoRows.size(), expectedPairs.size());

            long sharedSeqNum = expectedPairs.get(0)[1];
            for (int i = 0; i < prestoRows.size(); i++) {
                Long prestoRowId = (Long) prestoRows.get(i).getField(0);
                Long prestoSeqNum = (Long) prestoRows.get(i).getField(1);
                assertEquals(prestoRowId.longValue(), expectedPairs.get(i)[0],
                        "_row_id should match Iceberg metadata");
                assertEquals(prestoSeqNum.longValue(), expectedPairs.get(i)[1],
                        "_last_updated_sequence_number should match Iceberg metadata");
                // All rows in the same commit share one sequence number
                assertEquals(prestoSeqNum.longValue(), sharedSeqNum,
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
        String tableName = "test_native_row_lineage_v2_" + RUN_ID;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);

        try {
            Schema schema = createTestSchema();
            Table table = createTestTable(catalog, tableId, "2");

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"));
            table.refresh();
            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 3, "value", "three"));

            // V1/V2 tables have no row lineage; both columns are null.
            assertEquals(computeActual("SELECT \"_row_id\", * FROM " + tableName).getRowCount(), 3);
            assertQuery("SELECT \"_row_id\" FROM " + tableName + " ORDER BY id");
            assertQuery("SELECT \"_last_updated_sequence_number\" FROM " + tableName + " ORDER BY id", "VALUES 1, 1, 2");
            table.refresh();
            table.updateProperties()
                    .set("format-version", "3")
                    .commit();
            table.refresh();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 4, "value", "four"),
                    GenericRecord.create(schema).copy("id", 5, "value", "five"));
            table.refresh();

            assertQuery("SELECT \"_row_id\", \"_last_updated_sequence_number\", * FROM " + tableName);

            // V2→V3 upgrade backfills firstRowId on existing manifest entries, so all rows
            // (including the 3 pre-upgrade ones) now have non-null row lineage.
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
            List<long[]> allExpectedPairs = buildExpectedPairs(table, "All files should have firstRowId set after V3 upgrade");

            MaterializedResult postUpgradeResult = computeActual(
                    "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                            " ORDER BY \"_row_id\"");
            List<MaterializedRow> postUpgradeRows = postUpgradeResult.getMaterializedRows();
            assertEquals(postUpgradeRows.size(), allExpectedPairs.size());
            for (int i = 0; i < postUpgradeRows.size(); i++) {
                Long prestoRowId = (Long) postUpgradeRows.get(i).getField(0);
                Long prestoSeqNum = (Long) postUpgradeRows.get(i).getField(1);
                assertEquals(prestoRowId.longValue(), allExpectedPairs.get(i)[0],
                        "_row_id should match Iceberg metadata");
                assertEquals(prestoSeqNum.longValue(), allExpectedPairs.get(i)[1],
                        "_last_updated_sequence_number should match Iceberg metadata");
            }
        }
        finally {
            try {
                catalog.dropTable(tableId, true);
            }
            catch (Exception ignored) {
            }
        }
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
                long seqNum = dataFile.dataSequenceNumber();
                for (long pos = 0; pos < dataFile.recordCount(); pos++) {
                    pairs.add(new long[] {firstRowId + pos, seqNum});
                }
            }
        }
        pairs.sort((a, b) -> Long.compare(a[0], b[0]));
        return pairs;
    }

    private static Schema createTestSchema()
    {
        return new Schema(
                Types.NestedField.required(1, "id", Types.IntegerType.get()),
                Types.NestedField.optional(2, "value", Types.StringType.get()));
    }

    private static Table createTestTable(Catalog catalog, TableIdentifier tableId, String formatVersion)
    {
        return catalog.createTable(
                tableId,
                createTestSchema(),
                org.apache.iceberg.PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", formatVersion));
    }

    private void writeRecords(Table table, Record... records)
            throws Exception
    {
        String filename = "data-" + UUID.randomUUID() + ".parquet";
        org.apache.hadoop.fs.Path filePath = new org.apache.hadoop.fs.Path(
                table.location(), "data/" + filename);
        Configuration conf = new Configuration();

        DataWriter<Record> writer = Parquet.writeData(
                        HadoopOutputFile.fromPath(filePath, conf))
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

        table.newAppend()
                .appendFile(writer.toDataFile())
                .commit();
    }

    private Catalog loadCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), IcebergQueryRunner.ICEBERG_CATALOG,
                getProperties(), new Configuration());
    }

    private Map<String, String> getProperties()
    {
        File metastoreDir = getCatalogDirectory();
        return ImmutableMap.of("warehouse", metastoreDir.toURI().toString());
    }

    private File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner()
                .getCoordinator().getDataDirectory();
        Path catalogDirectory = IcebergQueryRunner.getIcebergDataDirectoryPath(
                dataDirectory, CatalogType.HADOOP.name(),
                new IcebergConfig().getFileFormat(), true);
        return catalogDirectory.toFile();
    }
}
