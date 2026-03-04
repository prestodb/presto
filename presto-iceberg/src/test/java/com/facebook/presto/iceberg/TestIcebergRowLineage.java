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
import java.util.OptionalInt;
import java.util.UUID;

import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests that row lineage values (_row_id and _last_updated_sequence_number) are consistent
 * between the Iceberg API (used by Spark internally to create and read Iceberg V3 tables)
 * and Presto (which reads the row lineage hidden columns).
 *
 * <p>This test creates V3 tables and writes data using the same Iceberg API that Spark uses
 * under the hood (HadoopCatalog, GenericRecord, GenericParquetWriter), then verifies that
 * Presto returns identical row lineage values to those derived from the Iceberg file metadata.
 */
public class TestIcebergRowLineage
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(HADOOP)
                .setFormat(FileFormat.PARQUET)
                .setNodeCount(OptionalInt.of(1))
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build().getQueryRunner();
    }

    @Test
    public void testSparkCreatedV3TableRowLineageMatchesPresto()
            throws Exception
    {
        String tableName = "test_spark_row_lineage";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);

        try {
            // Create V3 table using the Iceberg Catalog API (same API Spark's SparkCatalog uses)
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "value", Types.StringType.get()));
            Table table = catalog.createTable(tableId, schema,
                    org.apache.iceberg.PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));

            // First commit: write one row using Iceberg's data writer (same API Spark uses internally)
            writeRecords(table, schema, GenericRecord.create(schema).copy("id", 1, "value", "one"));

            // Second commit: write another row in a separate commit
            table.refresh();
            writeRecords(table, schema, GenericRecord.create(schema).copy("id", 2, "value", "two"));

            // Read expected row lineage values from Iceberg DataFile metadata
            // This is the ground truth that both Spark and Presto should agree on
            table.refresh();
            List<long[]> expectedPairs = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    DataFile dataFile = task.file();
                    Long firstRowId = dataFile.firstRowId();
                    long seqNum = task.file().dataSequenceNumber();

                    // Verify that V3 metadata was written correctly
                    assertNotNull(firstRowId,
                            "Iceberg should set firstRowId for V3 tables (same behavior as Spark)");

                    for (long pos = 0; pos < dataFile.recordCount(); pos++) {
                        expectedPairs.add(new long[] {firstRowId + pos, seqNum});
                    }
                }
            }
            expectedPairs.sort((a, b) -> Long.compare(a[0], b[0]));

            // Read row lineage from Presto
            MaterializedResult prestoResult = computeActual(
                    "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                            " ORDER BY \"_row_id\"");
            List<MaterializedRow> prestoRows = prestoResult.getMaterializedRows();

            // Assert same number of rows
            assertEquals(prestoRows.size(), expectedPairs.size(),
                    "Presto and Iceberg API should return the same number of rows");

            // Assert row lineage values match between Iceberg API (Spark path) and Presto
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

            // Verify _row_id uniqueness
            long distinctRowIds = (Long) computeActual(
                    "SELECT count(DISTINCT \"_row_id\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctRowIds, 2L, "Row IDs must be unique across all rows");

            // Verify sequence numbers differ between commits
            long distinctSeqNums = (Long) computeActual(
                    "SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctSeqNums, 2L, "Sequence numbers should differ between commits");

            // Verify ordering: earlier commits have smaller sequence numbers
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
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testSparkCreatedV3TableRowLineageWithMultipleRowsPerCommit()
            throws Exception
    {
        String tableName = "test_spark_row_lineage_multi";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);

        try {
            // Create V3 table
            Schema schema = new Schema(
                    Types.NestedField.required(1, "id", Types.IntegerType.get()),
                    Types.NestedField.optional(2, "value", Types.StringType.get()));
            Table table = catalog.createTable(tableId, schema,
                    org.apache.iceberg.PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));

            // Write multiple rows in a single commit (same as Spark INSERT with multiple values)
            writeRecords(table, schema,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"),
                    GenericRecord.create(schema).copy("id", 3, "value", "three"));

            // Read file metadata from Iceberg API
            table.refresh();
            List<long[]> expectedPairs = new ArrayList<>();
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    DataFile dataFile = task.file();
                    Long firstRowId = dataFile.firstRowId();
                    long seqNum = task.file().dataSequenceNumber();
                    assertNotNull(firstRowId, "firstRowId should be set for V3 tables");
                    for (long pos = 0; pos < dataFile.recordCount(); pos++) {
                        expectedPairs.add(new long[] {firstRowId + pos, seqNum});
                    }
                }
            }
            expectedPairs.sort((a, b) -> Long.compare(a[0], b[0]));

            // Read from Presto
            MaterializedResult prestoResult = computeActual(
                    "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " +
                            tableName + " ORDER BY \"_row_id\"");
            List<MaterializedRow> prestoRows = prestoResult.getMaterializedRows();

            assertEquals(prestoRows.size(), expectedPairs.size());

            // All rows from the same commit should have the same sequence number
            long firstSeqNum = expectedPairs.get(0)[1];
            for (int i = 0; i < prestoRows.size(); i++) {
                Long prestoRowId = (Long) prestoRows.get(i).getField(0);
                Long prestoSeqNum = (Long) prestoRows.get(i).getField(1);

                assertEquals(prestoRowId.longValue(), expectedPairs.get(i)[0],
                        "Row ID should match expected value from Iceberg metadata");
                assertEquals(prestoSeqNum.longValue(), expectedPairs.get(i)[1],
                        "Sequence number should match expected value from Iceberg metadata");
                assertEquals(prestoSeqNum.longValue(), firstSeqNum,
                        "All rows in the same commit should have the same sequence number");
            }

            // Verify all row IDs are unique
            long distinctRowIds = (Long) computeActual(
                    "SELECT count(DISTINCT \"_row_id\") FROM " + tableName).getOnlyValue();
            assertEquals(distinctRowIds, 3L);
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    private void writeRecords(Table table, Schema schema, Record... records)
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
                HadoopCatalog.class.getName(), ICEBERG_CATALOG,
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
        Path catalogDirectory = getIcebergDataDirectoryPath(
                dataDirectory, HADOOP.name(),
                new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }
}
