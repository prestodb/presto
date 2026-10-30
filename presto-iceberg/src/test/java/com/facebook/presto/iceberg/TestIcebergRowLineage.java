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
import com.facebook.presto.testing.QueryRunner;
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
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.iceberg.CatalogType.HADOOP;
import static com.facebook.presto.iceberg.IcebergQueryRunner.getIcebergDataDirectoryPath;
import static com.facebook.presto.iceberg.TypeConverter.ORC_ICEBERG_ID_KEY;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestIcebergRowLineage
        extends TestIcebergRowLineageBase
{
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

    @Override
    protected File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = getIcebergDataDirectoryPath(
                dataDirectory, HADOOP.name(), new IcebergConfig().getFileFormat(), false);
        return catalogDirectory.toFile();
    }

    @Test
    public void testPredicatePushdownPreCompaction()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_pre";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            List<long[]> idAndSeq = readIdAndSequenceNumber(tableName);
            assertEquals(idAndSeq.size(), 3);
            long seq1 = sequenceNumberForId(idAndSeq, 1);
            long seq2 = sequenceNumberForId(idAndSeq, 2);
            long seq3 = sequenceNumberForId(idAndSeq, 3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            assertIdsForPredicate(tableName, "<= " + seq1, ImmutableList.of(1));
            assertIdsForPredicate(tableName, "<= " + seq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(tableName, "<= " + seq3, ImmutableList.of(1, 2, 3));
            assertIdsForPredicate(tableName, "< " + seq1, ImmutableList.of());
            assertIdsForPredicate(tableName, "BETWEEN " + seq2 + " AND " + seq3, ImmutableList.of(2, 3));
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testPredicatePushdownPostCompaction()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_post";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            table.refresh();
            long preSeq1 = sequenceNumberForId(readIdAndSequenceNumber(tableName), 1);
            long preSeq2 = sequenceNumberForId(readIdAndSequenceNumber(tableName), 2);
            assertTrue(preSeq1 < preSeq2);

            Set<DataFile> preCompactionFiles = new HashSet<>();
            try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
                for (FileScanTask task : tasks) {
                    preCompactionFiles.add(task.file());
                }
            }
            assertEquals(preCompactionFiles.size(), 2);

            Schema lineageAugmentedSchema = MetadataColumns.schemaWithRowLineage(table.schema());
            Record row1 = GenericRecord.create(lineageAugmentedSchema);
            row1.setField("id", 1);
            row1.setField("value", "one");
            row1.setField(MetadataColumns.ROW_ID.name(), 0L);
            row1.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), preSeq1);
            Record row2 = GenericRecord.create(lineageAugmentedSchema);
            row2.setField("id", 2);
            row2.setField("value", "two");
            row2.setField(MetadataColumns.ROW_ID.name(), 1L);
            row2.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), preSeq2);
            DataFile compactedFile = writeFileWithSchema(table, lineageAugmentedSchema, row1, row2);

            Set<DataFile> compactedFiles = new HashSet<>();
            compactedFiles.add(compactedFile);
            table.newRewrite()
                    .rewriteFiles(preCompactionFiles, compactedFiles)
                    .commit();

            List<long[]> postIdAndSeq = readIdAndSequenceNumber(tableName);
            assertEquals(postIdAndSeq.size(), 2);
            assertEquals(sequenceNumberForId(postIdAndSeq, 1), preSeq1);
            assertEquals(sequenceNumberForId(postIdAndSeq, 2), preSeq2);

            int lineageFieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
            table.refresh();
            DataFile committedFile = null;
            try (CloseableIterable<FileScanTask> tasks = table.newScan().includeColumnStats().planFiles()) {
                for (FileScanTask t : tasks) {
                    committedFile = t.file();
                }
            }
            assertTrue(committedFile != null
                            && committedFile.lowerBounds() != null
                            && committedFile.lowerBounds().containsKey(lineageFieldId),
                    "compaction file is missing lineage column lower bound stats");

            assertIdsForPredicate(tableName, "<= " + preSeq1, ImmutableList.of(1));
            assertIdsForPredicate(tableName, "<= " + preSeq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(tableName, "< " + preSeq1, ImmutableList.of());
            assertIdsForPredicate(tableName, "> " + preSeq2, ImmutableList.of());
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testV2TableLineagePredicates()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_v2";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "2"));
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");

            assertIdsForPredicate(tableName, "<= 100", ImmutableList.of());
            assertIdsForPredicate(tableName, "> 0", ImmutableList.of());
            assertIdsForPredicate(tableName, "IS NOT NULL", ImmutableList.of());
            assertIdsForPredicate(tableName, "IS NULL", ImmutableList.of(1, 2));
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testPredicateActuallyPrunesSplits()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_split_count";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            long minSeq = sequenceNumberForId(readIdAndSequenceNumber(tableName), 1);

            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsPruned = completedSplitsFor(
                    "SELECT id FROM " + tableName +
                            " WHERE \"_last_updated_sequence_number\" < " + minSeq);

            assertTrue(splitsAll > splitsPruned,
                    "expected predicate to prune splits but unrestricted=" + splitsAll
                            + " pruned=" + splitsPruned);
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testDisjointOrRangesPruneMiddleFile()
            throws Exception
    {
        String tableName = "test_lineage_disjoint_or";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            List<long[]> idAndSeq = readIdAndSequenceNumber(tableName);
            long seq1 = sequenceNumberForId(idAndSeq, 1);
            long seq2 = sequenceNumberForId(idAndSeq, 2);
            long seq3 = sequenceNumberForId(idAndSeq, 3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            String disjointPredicate = " WHERE \"_last_updated_sequence_number\" <= " + seq1
                    + " OR \"_last_updated_sequence_number\" >= " + seq3;

            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsDisjoint = completedSplitsFor("SELECT id FROM " + tableName + disjointPredicate);
            assertTrue(splitsAll > splitsDisjoint,
                    "expected disjoint OR to prune middle file but unrestricted=" + splitsAll
                            + " disjoint=" + splitsDisjoint);

            MaterializedResult result = computeActual("SELECT id FROM " + tableName + disjointPredicate + " ORDER BY id");
            List<Integer> ids = new ArrayList<>();
            for (MaterializedRow row : result.getMaterializedRows()) {
                ids.add((Integer) row.getField(0));
            }
            assertEquals(ids, ImmutableList.of(1, 3));
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @DataProvider(name = "boundlessLineageMetricsMode")
    public Object[][] boundlessLineageMetricsModeProvider()
    {
        return new Object[][] {
                {"counts"},
                {"none"}
        };
    }

    @DataProvider(name = "fileFormat")
    public Object[][] fileFormatProvider()
    {
        return new Object[][] {
                {FileFormat.PARQUET},
                {FileFormat.ORC}
        };
    }

    /**
     * A compacted file whose lineage columns have no bounds, only counts or no metrics at all,
     * stores values the coordinator cannot see, so it must not be pruned by its own, newer data
     * sequence number.
     */
    @Test(dataProvider = "boundlessLineageMetricsMode")
    public void testCompactedFileWithoutLineageBoundsIsNotMisPruned(String lineageMetricsMode)
            throws Exception
    {
        String tableName = "test_lineage_metrics_" + lineageMetricsMode;
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            List<long[]> idAndRowId = readIdAndLongColumn(getSession(), tableName, "_row_id");
            List<long[]> idAndSequenceNumber = readIdAndSequenceNumber(tableName);
            long preCompactionSequenceNumber1 = valueForId(idAndSequenceNumber, 1);
            long preCompactionSequenceNumber2 = valueForId(idAndSequenceNumber, 2);
            List<DataFile> preCompactionFiles = dataFiles(table);

            Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
            Record row1 = GenericRecord.create(lineageSchema);
            row1.setField("id", 1);
            row1.setField("value", "one");
            row1.setField(MetadataColumns.ROW_ID.name(), valueForId(idAndRowId, 1));
            row1.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), preCompactionSequenceNumber1);
            Record row2 = GenericRecord.create(lineageSchema);
            row2.setField("id", 2);
            row2.setField("value", "two");
            row2.setField(MetadataColumns.ROW_ID.name(), valueForId(idAndRowId, 2));
            row2.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), preCompactionSequenceNumber2);
            MetricsConfig boundlessLineage = MetricsConfig.fromProperties(ImmutableMap.of(
                    "write.metadata.metrics.column." + MetadataColumns.ROW_ID.name(), lineageMetricsMode,
                    "write.metadata.metrics.column." + MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), lineageMetricsMode));
            DataFile compacted = writeFileWithMetrics(table, lineageSchema, boundlessLineage, row1, row2);
            table.newRewrite()
                    .rewriteFiles(ImmutableSet.copyOf(preCompactionFiles), ImmutableSet.of(compacted))
                    .commit();
            table.refresh();

            int lineageFieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
            DataFile committed = getOnlyElement(dataFiles(table));
            assertEquals(committed.valueCounts() != null && committed.valueCounts().containsKey(lineageFieldId), lineageMetricsMode.equals("counts"),
                    "compacted file should carry lineage value counts only in metrics mode counts");
            assertTrue(committed.lowerBounds() == null || !committed.lowerBounds().containsKey(lineageFieldId),
                    "compacted file should carry no lineage bounds");
            assertTrue(committed.dataSequenceNumber() > preCompactionSequenceNumber2, "the rewrite must commit at a newer sequence number");

            assertIdsForPredicate(tableName, "= " + preCompactionSequenceNumber1, ImmutableList.of(1));
            assertIdsForPredicate(tableName, "= " + preCompactionSequenceNumber2, ImmutableList.of(2));
            assertIdsForPredicate(tableName, "<= " + preCompactionSequenceNumber2, ImmutableList.of(1, 2));
            assertIdsForPredicate(tableName, "= " + committed.dataSequenceNumber(), ImmutableList.of());
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * A file without column metrics that an APPEND snapshot added is pruned by its data sequence
     * number, since an append stores no lineage values. The predicate admits values below the second
     * file's sequence number, so only the APPEND snapshot lets that file be pruned.
     */
    @Test
    public void testAppendedFileWithoutMetricsIsPruned()
            throws Exception
    {
        String tableName = "test_lineage_append_without_metrics";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3", DEFAULT_WRITE_METRICS_MODE, "none"));
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            assertNoColumnMetrics(table);
            long sequenceNumber1 = sequenceNumberForId(readIdAndSequenceNumber(tableName), 1);

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + sequenceNumber1;
            assertIdsForQuery(getSession(), selective, ImmutableList.of(1));
            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsSelective = completedSplitsFor(selective);
            assertTrue(splitsAll > splitsSelective,
                    "expected the second appended file to be pruned but unrestricted=" + splitsAll + " selective=" + splitsSelective);
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    /**
     * No row's value exceeds its file's sequence number, so a predicate above it prunes a file without
     * lineage metrics even after the APPEND snapshot that added the file is expired.
     */
    @Test
    public void testFileFromExpiredAppendIsPrunedAboveItsSequenceNumber()
            throws Exception
    {
        String tableName = "test_lineage_expired_append_pruned";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3", DEFAULT_WRITE_METRICS_MODE, "none"));
            appendOneRow(table, 1, "one");
            long firstAppendSnapshotId = table.currentSnapshot().snapshotId();
            appendOneRow(table, 2, "two");
            assertNoColumnMetrics(table);
            long sequenceNumber1 = sequenceNumberForId(readIdAndSequenceNumber(tableName), 1);
            table.expireSnapshots().expireSnapshotId(firstAppendSnapshotId).commit();
            table.refresh();

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" > " + sequenceNumber1;
            assertIdsForQuery(getSession(), selective, ImmutableList.of(2));
            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsSelective = completedSplitsFor(selective);
            assertTrue(splitsAll > splitsSelective,
                    "expected the file from the expired append to be pruned but unrestricted=" + splitsAll + " selective=" + splitsSelective);
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testLineagePredicateWithDeletedRow()
            throws Exception
    {
        // AUTOMATIC join distribution computes stats for the equality delete scan, which plans its
        // delete files with the data scan's predicate, lineage domains included.
        Session session = automaticJoinSession();
        String tableName = "test_lineage_deleted_row";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));
            Schema schema = table.schema();
            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 1, "value", "one"),
                    GenericRecord.create(schema).copy("id", 2, "value", "two"),
                    GenericRecord.create(schema).copy("id", 3, "value", "three"));
            table.refresh();
            DataFile file = getOnlyElement(dataFiles(table));
            long firstRowId = file.firstRowId();
            writeEqualityDeleteOnId(table, 2);

            List<long[]> idAndRowId = readIdAndLongColumn(session, tableName, "_row_id");
            assertEquals(idAndRowId.size(), 2);
            assertEquals(valueForId(idAndRowId, 1), firstRowId);
            assertEquals(valueForId(idAndRowId, 3), firstRowId + 2);

            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + (firstRowId + 2), ImmutableList.of(3));
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + (firstRowId + 1), ImmutableList.of());
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + file.dataSequenceNumber(), ImmutableList.of(1, 3));
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testTableStatisticsWithLineagePredicate()
            throws Exception
    {
        // Statistics for the data scan and for the equality delete scan are computed with the
        // layout's predicate, lineage domains included, and Iceberg cannot bind lineage columns
        // when it plans either scan. AUTOMATIC join distribution makes the planner compute them.
        // SHOW STATS cannot cover this: a lineage predicate is also filtered per row, and SHOW
        // STATS rejects predicates the connector does not enforce.
        Session session = automaticJoinSession();
        String tableName = "test_table_statistics_lineage_predicate";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));
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
            catalog.dropTable(tableId, true);
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
    public void testRowIdPredicatesOnMixedStoredAndInheritedFile(FileFormat fileFormat)
            throws Exception
    {
        String tableName = "test_lineage_mixed_file";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            MixedLineageTuple mixed = createMixedLineageTable(catalog, tableId, tableName, fileFormat);

            assertIdsForQuery(getSession(), "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + mixed.inheritedRowId, ImmutableList.of(2));
            assertIdsForQuery(getSession(), "SELECT id FROM " + tableName + " WHERE \"_row_id\" = " + mixed.storedRowId, ImmutableList.of(1));
            assertIdsForQuery(getSession(), "SELECT id FROM " + tableName + " WHERE \"_row_id\" >= " + mixed.inheritedRowId, ImmutableList.of(2));
        }
        finally {
            catalog.dropTable(tableId, true);
        }
    }

    @Test
    public void testIsNullOnV3Table()
            throws Exception
    {
        String inheritedTableName = "test_lineage_is_null_inherited";
        String mixedTableName = "test_lineage_is_null_mixed";
        Catalog catalog = loadCatalog();
        TableIdentifier inheritedTableId = TableIdentifier.of(TEST_SCHEMA, inheritedTableName);
        TableIdentifier mixedTableId = TableIdentifier.of(TEST_SCHEMA, mixedTableName);
        try {
            Table inherited = catalog.createTable(inheritedTableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                    ImmutableMap.of("format-version", "3"));
            appendOneRow(inherited, 1, "one");
            appendOneRow(inherited, 2, "two");
            createMixedLineageTable(catalog, mixedTableId, mixedTableName, FileFormat.PARQUET);

            for (String tableName : ImmutableList.of(inheritedTableName, mixedTableName)) {
                for (String column : ImmutableList.of("_row_id", "_last_updated_sequence_number")) {
                    assertIdsForQuery(getSession(), "SELECT id FROM " + tableName + " WHERE \"" + column + "\" IS NULL", ImmutableList.of());
                    assertIdsForQuery(getSession(), "SELECT id FROM " + tableName + " WHERE \"" + column + "\" IS NOT NULL", ImmutableList.of(1, 2));
                }
            }
        }
        finally {
            catalog.dropTable(inheritedTableId, true);
            catalog.dropTable(mixedTableId, true);
        }
    }

    private static final Schema PUSHDOWN_TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));

    private void assertIdsForPredicate(String tableName, String predicate, List<Integer> expectedIds)
    {
        MaterializedResult result = computeActual(
                "SELECT id FROM " + tableName +
                        " WHERE \"_last_updated_sequence_number\" " + predicate +
                        " ORDER BY id");
        List<Integer> actualIds = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            actualIds.add((Integer) row.getField(0));
        }
        assertEquals(actualIds, expectedIds, "rows for predicate \"" + predicate + "\"");
    }

    private List<long[]> readIdAndSequenceNumber(String tableName)
    {
        MaterializedResult result = computeActual(
                "SELECT id, \"_last_updated_sequence_number\" FROM " + tableName + " ORDER BY id");
        List<long[]> rows = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            rows.add(new long[] {(Integer) row.getField(0), (Long) row.getField(1)});
        }
        return rows;
    }

    private static long sequenceNumberForId(List<long[]> rows, int id)
    {
        for (long[] row : rows) {
            if (row[0] == id) {
                return row[1];
            }
        }
        throw new AssertionError("id not found: " + id);
    }

    private void appendOneRow(Table table, int id, String value)
            throws Exception
    {
        Record record = GenericRecord.create(table.schema());
        record.setField("id", id);
        record.setField("value", value);
        DataFile dataFile = writeFileWithSchema(table, table.schema(), record);
        table.newAppend().appendFile(dataFile).commit();
        table.refresh();
    }

    private DataFile writeFileWithSchema(Table table, Schema writeSchema, Record... records)
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
                .metricsConfig(org.apache.iceberg.MetricsConfig.forTable(table))
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

    private int completedSplitsFor(String sql)
    {
        DistributedQueryRunner runner = (DistributedQueryRunner) getQueryRunner();
        ResultWithQueryId<MaterializedResult> result = runner.executeWithQueryId(getSession(), sql);
        QueryStats stats = runner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(result.getQueryId())
                .getQueryStats();
        return stats.getCompletedSplits();
    }

    /**
     * {@code sql} must select {@code id} as its first column.
     */
    private void assertIdsForQuery(Session session, String sql, List<Integer> expectedIds)
    {
        List<Integer> actualIds = new ArrayList<>();
        for (MaterializedRow row : computeActual(session, sql).getMaterializedRows()) {
            actualIds.add((Integer) row.getField(0));
        }
        assertEquals(ImmutableList.sortedCopyOf(actualIds), ImmutableList.sortedCopyOf(expectedIds), "rows for \"" + sql + "\"");
    }

    private List<long[]> readIdAndLongColumn(Session session, String tableName, String column)
    {
        List<long[]> rows = new ArrayList<>();
        for (MaterializedRow row : computeActual(session, "SELECT id, \"" + column + "\" FROM " + tableName).getMaterializedRows()) {
            rows.add(new long[] {(Integer) row.getField(0), (Long) row.getField(1)});
        }
        return rows;
    }

    private static long valueForId(List<long[]> rows, int id)
    {
        for (long[] row : rows) {
            if (row[0] == id) {
                return row[1];
            }
        }
        throw new AssertionError("id not found: " + id);
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

    private static void assertNoColumnMetrics(Table table)
            throws Exception
    {
        for (DataFile file : dataFiles(table)) {
            assertTrue(file.valueCounts() == null || file.valueCounts().isEmpty(), "file should carry no column metrics: " + file.location());
        }
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

    private Session automaticJoinSession()
    {
        return Session.builder(getSession())
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
    private MixedLineageTuple createMixedLineageTable(Catalog catalog, TableIdentifier tableId, String tableName, FileFormat fileFormat)
            throws Exception
    {
        Table table = catalog.createTable(tableId, PUSHDOWN_TABLE_SCHEMA, PartitionSpec.unpartitioned(),
                ImmutableMap.of("format-version", "3"));
        appendOneRow(table, 1, "one");
        appendOneRow(table, 2, "two");
        long storedRowId = valueForId(readIdAndLongColumn(getSession(), tableName, "_row_id"), 1);
        long storedSequenceNumber = valueForId(readIdAndLongColumn(getSession(), tableName, "_last_updated_sequence_number"), 1);

        Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
        Record stored = GenericRecord.create(lineageSchema);
        stored.setField("id", 1);
        stored.setField("value", "one");
        stored.setField(MetadataColumns.ROW_ID.name(), storedRowId);
        stored.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), storedSequenceNumber);
        Record inherited = GenericRecord.create(lineageSchema);
        inherited.setField("id", 2);
        inherited.setField("value", "two-updated");
        OverwriteFiles overwrite = table.newOverwrite();
        for (DataFile file : dataFiles(table)) {
            overwrite.deleteFile(file);
        }
        overwrite.addFile(writeFileInFormat(table, lineageSchema, fileFormat, stored, inherited)).commit();
        table.refresh();

        DataFile file = getOnlyElement(dataFiles(table));
        MixedLineageTuple mixed = new MixedLineageTuple(storedRowId, storedSequenceNumber, file.firstRowId() + 1, file.dataSequenceNumber());
        assertTrue(mixed.inheritedSequenceNumber > mixed.storedSequenceNumber, "the rewrite must commit at a newer sequence number");

        List<long[]> idAndRowId = readIdAndLongColumn(getSession(), tableName, "_row_id");
        assertEquals(valueForId(idAndRowId, 1), mixed.storedRowId);
        assertEquals(valueForId(idAndRowId, 2), mixed.inheritedRowId);
        List<long[]> idAndSequenceNumber = readIdAndLongColumn(getSession(), tableName, "_last_updated_sequence_number");
        assertEquals(valueForId(idAndSequenceNumber, 1), mixed.storedSequenceNumber);
        assertEquals(valueForId(idAndSequenceNumber, 2), mixed.inheritedSequenceNumber);
        return mixed;
    }

    private DataFile writeFileInFormat(Table table, Schema writeSchema, FileFormat fileFormat, Record... records)
            throws Exception
    {
        if (fileFormat == FileFormat.PARQUET) {
            return writeFileWithSchema(table, writeSchema, records);
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
