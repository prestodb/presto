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
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.ResultWithQueryId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.DataFile;
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
import org.apache.iceberg.hadoop.HadoopOutputFile;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.javaIcebergQueryRunnerBuilder;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.nativeIcebergQueryRunnerBuilder;
import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

/**
 * Tests for Iceberg Format Version 3 row lineage ({@code _row_id} / {@code _last_updated_sequence_number}).
 *
 * Tables are written through the Iceberg API, because Presto DML cannot produce stored lineage values,
 * compaction output, or a V2 to V3 upgrade. They are read through the native query runner and the Java
 * expected query runner, which share one HADOOP catalog directory. Every test method is parameterized
 * with {@code pushdown_filter_enabled=true/false} for the native side. The Java side always runs with
 * pushdown disabled, since the Java Iceberg connector rejects filter pushdown.
 */
public class TestIcebergV3RowLineage
        extends AbstractTestQueryFramework
{
    private static final String TEST_SCHEMA = "tpch";

    private static final Schema TEST_TABLE_SCHEMA = new Schema(
            Types.NestedField.required(1, "id", Types.IntegerType.get()),
            Types.NestedField.optional(2, "value", Types.StringType.get()));

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return nativeIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return javaIcebergQueryRunnerBuilder()
                .setStorageFormat(ICEBERG_DEFAULT_STORAGE_FORMAT)
                .setCatalogType(CatalogType.HADOOP)
                .setAddStorageFormatToPath(true)
                .build();
    }

    @DataProvider(name = "pushdownFilterEnabled")
    public static Object[][] pushdownFilterEnabledProvider()
    {
        return new Object[][] {
                {true},
                {false}
        };
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

            assertLineageMatchesIcebergMetadata(session, tableName,
                    buildExpectedPairs(table, "Iceberg should set firstRowId for V3 tables"));

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(session, distinctRowIdsSql, sessionWithPushdown(false), distinctRowIdsSql);
            assertEquals(computeScalar(session, distinctRowIdsSql), 2L, "Row IDs must be unique across all rows");

            String distinctSeqNumsSql = "SELECT count(DISTINCT \"_last_updated_sequence_number\") FROM " + tableName;
            assertQuery(session, distinctSeqNumsSql, sessionWithPushdown(false), distinctSeqNumsSql);
            assertEquals(computeScalar(session, distinctSeqNumsSql), 2L, "Sequence numbers should differ between commits");

            List<long[]> idAndSeq = readIdAndSequenceNumber(session, tableName);
            assertTrue(valueForId(idAndSeq, 1) < valueForId(idAndSeq, 2),
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
            assertLineageMatchesIcebergMetadata(session, tableName, expectedPairs);

            long sharedSeqNum = expectedPairs.get(0)[1];
            for (long[] pair : expectedPairs) {
                assertEquals(pair[1], sharedSeqNum, "All rows in a single commit should have the same sequence number");
            }

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(session, distinctRowIdsSql, sessionWithPushdown(false), distinctRowIdsSql);
            assertEquals(computeScalar(session, distinctRowIdsSql), 3L, "Row IDs must be unique across all rows");
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
            table.refresh();

            String allRowsSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName;
            assertQuery(session, allRowsSql, sessionWithPushdown(false), allRowsSql);
            assertEquals(computeActual(session, allRowsSql).getRowCount(), 3);
            for (String column : ImmutableList.of("_row_id", "_last_updated_sequence_number")) {
                assertIdsForPredicate(session, tableName, column, "IS NOT NULL", ImmutableList.of());
                assertIdsForPredicate(session, tableName, column, "IS NULL", ImmutableList.of(1, 2, 3));
            }

            table.updateProperties().set("format-version", "3").commit();
            table.refresh();

            writeRecords(table,
                    GenericRecord.create(schema).copy("id", 4, "value", "four"),
                    GenericRecord.create(schema).copy("id", 5, "value", "five"));
            table.refresh();

            for (String column : ImmutableList.of("_row_id", "_last_updated_sequence_number")) {
                assertIdsForPredicate(session, tableName, column, "IS NULL", ImmutableList.of());
                assertIdsForPredicate(session, tableName, column, "IS NOT NULL", ImmutableList.of(1, 2, 3, 4, 5));
            }

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(session, distinctRowIdsSql, sessionWithPushdown(false), distinctRowIdsSql);
            assertEquals(computeScalar(session, distinctRowIdsSql), 5L, "Row IDs must be unique across all 5 rows after upgrade");

            assertLineageMatchesIcebergMetadata(session, tableName,
                    buildExpectedPairs(table, "All files should have firstRowId set after V3 upgrade"));
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

            writeOverriddenLineageRow(table);
            table.refresh();

            String unfilteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName;
            String filteredSql = unfilteredSql + " WHERE \"_row_id\" IS NOT NULL";
            assertQuery(session, unfilteredSql, sessionWithPushdown(false), unfilteredSql);
            assertQuery(session, filteredSql, sessionWithPushdown(false), filteredSql);

            MaterializedResult unfiltered = computeActual(session, unfilteredSql);
            MaterializedResult filtered = computeActual(session, filteredSql);

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
            dropTableQuietly(catalog, tableId);
        }
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

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            List<long[]> idAndSeq = readIdAndSequenceNumber(session, tableName);
            assertEquals(idAndSeq.size(), 3);
            long seq1 = valueForId(idAndSeq, 1);
            long seq2 = valueForId(idAndSeq, 2);
            long seq3 = valueForId(idAndSeq, 3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            assertIdsForPredicate(session, tableName, "<= " + seq1, ImmutableList.of(1));
            assertIdsForPredicate(session, tableName, "<= " + seq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(session, tableName, "<= " + seq3, ImmutableList.of(1, 2, 3));
            assertIdsForPredicate(session, tableName, "< " + seq1, ImmutableList.of());
            assertIdsForPredicate(session, tableName, "BETWEEN " + seq2 + " AND " + seq3, ImmutableList.of(2, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * Compaction rewrites two single-row files into one file that explicitly carries each row's
     * original {@code _row_id} / {@code _last_updated_sequence_number}, which is how lineage
     * survives a rewrite. The reader must report those physical values rather than the compacted
     * file's own firstRowId+position and dataSequenceNumber, and the lineage column statistics must
     * still allow the file to be pruned by a predicate.
     */
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

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            table.refresh();
            List<long[]> preIdAndSeq = readIdAndSequenceNumber(session, tableName);
            long preSeq1 = valueForId(preIdAndSeq, 1);
            long preSeq2 = valueForId(preIdAndSeq, 2);
            assertTrue(preSeq1 < preSeq2);

            Set<DataFile> preCompactionFiles = new HashSet<>(dataFiles(table));
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
            DataFile compactedFile = writeFile(table, lineageAugmentedSchema, row1, row2);

            table.newRewrite()
                    .rewriteFiles(preCompactionFiles, ImmutableSet.of(compactedFile))
                    .commit();
            table.refresh();

            List<long[]> postIdAndSeq = readIdAndSequenceNumber(session, tableName);
            assertEquals(postIdAndSeq.size(), 2);
            assertEquals(valueForId(postIdAndSeq, 1), preSeq1);
            assertEquals(valueForId(postIdAndSeq, 2), preSeq2);

            int lineageFieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
            DataFile committedFile = getOnlyElement(dataFiles(table));
            assertTrue(committedFile.lowerBounds() != null && committedFile.lowerBounds().containsKey(lineageFieldId),
                    "compaction file is missing lineage column lower bound stats");

            assertIdsForPredicate(session, tableName, "<= " + preSeq1, ImmutableList.of(1));
            assertIdsForPredicate(session, tableName, "<= " + preSeq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(session, tableName, "< " + preSeq1, ImmutableList.of());
            assertIdsForPredicate(session, tableName, "> " + preSeq2, ImmutableList.of());
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
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");

            assertIdsForPredicate(session, tableName, "<= 100", ImmutableList.of());
            assertIdsForPredicate(session, tableName, "> 0", ImmutableList.of());
            assertIdsForPredicate(session, tableName, "IS NOT NULL", ImmutableList.of());
            assertIdsForPredicate(session, tableName, "IS NULL", ImmutableList.of(1, 2));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * Split counts are asserted on the native runner only: split generation and metadata-based
     * pruning happen on the Java coordinator in both configurations, so comparing them across
     * engines would compare identical code against itself. The assertion stays relative so it is
     * insensitive to worker count.
     */
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

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            long minSeq = valueForId(readIdAndSequenceNumber(session, tableName), 1);

            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            int splitsPruned = completedSplitsFor(session,
                    "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" < " + minSeq);

            assertTrue(splitsAll > splitsPruned,
                    "expected predicate to prune splits but unrestricted=" + splitsAll + " pruned=" + splitsPruned);
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

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            List<long[]> idAndSeq = readIdAndSequenceNumber(session, tableName);
            long seq1 = valueForId(idAndSeq, 1);
            long seq2 = valueForId(idAndSeq, 2);
            long seq3 = valueForId(idAndSeq, 3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            String disjointSql = "SELECT id FROM " + tableName
                    + " WHERE \"_last_updated_sequence_number\" <= " + seq1
                    + " OR \"_last_updated_sequence_number\" >= " + seq3;

            int splitsAll = completedSplitsFor(session, "SELECT id FROM " + tableName);
            int splitsDisjoint = completedSplitsFor(session, disjointSql);
            assertTrue(splitsAll > splitsDisjoint,
                    "expected disjoint OR to prune middle file but unrestricted=" + splitsAll + " disjoint=" + splitsDisjoint);

            assertIdsForQuery(session, disjointSql, ImmutableList.of(1, 3));
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
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            // One row per commit, committed in id order, so ascending row ids map to ids 1, 2, 3.
            List<long[]> expectedPairs = buildExpectedPairs(table, "firstRowId should be set for V3 tables");
            long rowId1 = expectedPairs.get(0)[0];
            long rowId2 = expectedPairs.get(1)[0];
            long rowId3 = expectedPairs.get(2)[0];
            List<long[]> idAndRowId = readIdAndRowId(session, tableName);
            assertEquals(valueForId(idAndRowId, 1), rowId1);
            assertEquals(valueForId(idAndRowId, 2), rowId2);
            assertEquals(valueForId(idAndRowId, 3), rowId3);

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
            writeOverriddenLineageRow(table);
            table.refresh();

            DataFile storedFile = dataFiles(table).stream()
                    .filter(file -> file.lowerBounds() != null && file.lowerBounds().containsKey(MetadataColumns.ROW_ID.fieldId()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("file with stored lineage not found"));
            long inheritedRowId = storedFile.firstRowId();
            long inheritedSeq = storedFile.dataSequenceNumber();

            assertIdsForPredicate(session, tableName, "_row_id", "= 42", ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_row_id", "= " + inheritedRowId, ImmutableList.of());
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= 99", ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= " + inheritedSeq, ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test(dataProvider = "pushdownFilterEnabled")
    public void testPredicatesOnMixedStoredAndInheritedFile(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_mixed_file";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            MixedLineageTable mixed = createMixedLineageTable(session, catalog, tableId, tableName);

            assertIdsForPredicate(session, tableName, "_row_id", "= " + mixed.inheritedRowId, ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_row_id", "= " + mixed.storedRowId, ImmutableList.of(1));
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= " + mixed.inheritedSeq, ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", ">= " + mixed.inheritedSeq, ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= " + mixed.storedSeq, ImmutableList.of(1));
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
            appendOneRow(inherited, 1, "one");
            appendOneRow(inherited, 2, "two");
            createMixedLineageTable(session, catalog, mixedTableId, mixedTableName);

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
    public void testLineagePredicateWithDeletedRow(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
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

            List<long[]> idAndRowId = readIdAndRowId(session, tableName);
            assertEquals(idAndRowId.size(), 2);
            assertEquals(valueForId(idAndRowId, 1), firstRowId);
            assertEquals(valueForId(idAndRowId, 3), firstRowId + 2);

            assertIdsForPredicate(session, tableName, "_row_id", "= " + (firstRowId + 2), ImmutableList.of(3));
            assertIdsForPredicate(session, tableName, "_row_id", "= " + (firstRowId + 1), ImmutableList.of());
            assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", "= " + file.dataSequenceNumber(), ImmutableList.of(1, 3));
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
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");
            List<long[]> idAndRowId = readIdAndRowId(session, tableName);
            long rowId1 = valueForId(idAndRowId, 1);
            long seq2 = valueForId(readIdAndSequenceNumber(session, tableName), 2);
            ImmutableList.Builder<Integer> evenRowIds = ImmutableList.builder();
            for (long[] row : idAndRowId) {
                if (row[1] % 2 == 0) {
                    evenRowIds.add((int) row[0]);
                }
            }

            assertIdsForQuery(session, "SELECT id FROM " + tableName +
                            " WHERE \"_last_updated_sequence_number\" >= " + seq2 + " AND value <> 'three'",
                    ImmutableList.of(2));
            assertIdsForQuery(session, "SELECT id FROM " + tableName +
                            " WHERE \"_row_id\" = " + rowId1 + " OR id = 3",
                    ImmutableList.of(1, 3));
            assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"_row_id\" % 2 = 0",
                    evenRowIds.build());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * A compacted file whose lineage columns have value and null counts but no bounds stores
     * values the coordinator cannot see, so it must not be pruned by its own, newer data sequence
     * number.
     */
    @Test(dataProvider = "pushdownFilterEnabled")
    public void testCountsOnlyCompactedFileIsNotMisPruned(boolean pushdownFilterEnabled)
            throws Exception
    {
        Session session = sessionWithPushdown(pushdownFilterEnabled);
        String tableName = "test_lineage_counts_only";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            List<long[]> idAndRowId = readIdAndRowId(session, tableName);
            List<long[]> idAndSeq = readIdAndSequenceNumber(session, tableName);
            long preSeq1 = valueForId(idAndSeq, 1);
            long preSeq2 = valueForId(idAndSeq, 2);
            List<DataFile> preCompactionFiles = dataFiles(table);

            Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
            Record row1 = GenericRecord.create(lineageSchema);
            row1.setField("id", 1);
            row1.setField("value", "one");
            row1.setField(MetadataColumns.ROW_ID.name(), valueForId(idAndRowId, 1));
            row1.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), preSeq1);
            Record row2 = GenericRecord.create(lineageSchema);
            row2.setField("id", 2);
            row2.setField("value", "two");
            row2.setField(MetadataColumns.ROW_ID.name(), valueForId(idAndRowId, 2));
            row2.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), preSeq2);
            MetricsConfig countsOnlyLineage = MetricsConfig.fromProperties(ImmutableMap.of(
                    "write.metadata.metrics.column." + MetadataColumns.ROW_ID.name(), "counts",
                    "write.metadata.metrics.column." + MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), "counts"));
            DataFile compacted = writeFile(table, lineageSchema, countsOnlyLineage, row1, row2);
            table.newRewrite()
                    .rewriteFiles(ImmutableSet.copyOf(preCompactionFiles), ImmutableSet.of(compacted))
                    .commit();
            table.refresh();

            int lineageFieldId = MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.fieldId();
            DataFile committed = getOnlyElement(dataFiles(table));
            assertTrue(committed.valueCounts() != null && committed.valueCounts().containsKey(lineageFieldId),
                    "compacted file should carry lineage value counts");
            assertTrue(committed.lowerBounds() == null || !committed.lowerBounds().containsKey(lineageFieldId),
                    "compacted file should carry no lineage bounds");
            assertTrue(committed.dataSequenceNumber() > preSeq2, "the rewrite must commit at a newer sequence number");

            assertIdsForPredicate(session, tableName, "= " + preSeq1, ImmutableList.of(1));
            assertIdsForPredicate(session, tableName, "= " + preSeq2, ImmutableList.of(2));
            assertIdsForPredicate(session, tableName, "<= " + preSeq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(session, tableName, "= " + committed.dataSequenceNumber(), ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
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
            MixedLineageTable mixed = createMixedLineageTable(session, catalog, tableId, tableName);
            Table table = catalog.loadTable(tableId);
            appendOneRow(table, 3, "three");
            long seq3 = valueForId(readIdAndSequenceNumber(session, tableName), 3);
            assertTrue(seq3 > mixed.inheritedSeq);

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + seq3;
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

    private Session sessionWithPushdown(boolean pushdownFilterEnabled)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, PUSHDOWN_FILTER_ENABLED, Boolean.toString(pushdownFilterEnabled))
                .build();
    }

    private void assertIdsForPredicate(Session session, String tableName, String predicate, List<Integer> expectedIds)
    {
        assertIdsForPredicate(session, tableName, "_last_updated_sequence_number", predicate, expectedIds);
    }

    /**
     * Only {@code id} is selected, so {@code column} is read for the filter alone.
     */
    private void assertIdsForPredicate(Session session, String tableName, String column, String predicate, List<Integer> expectedIds)
    {
        assertIdsForQuery(session, "SELECT id FROM " + tableName + " WHERE \"" + column + "\" " + predicate, expectedIds);
    }

    /**
     * Compares {@code sql} on the native workers under {@code session} with the Java workers, then
     * checks the ids against {@code expectedIds}. That catches errors both engines share, such as
     * coordinator split pruning.
     */
    private void assertIdsForQuery(Session session, String sql, List<Integer> expectedIds)
    {
        assertQuery(session, sql, sessionWithPushdown(false), sql);
        assertEquals(sortedIdsOf(computeActual(session, sql)), expectedIds, "rows for \"" + sql + "\"");
    }

    /**
     * Verifies the table's {@code (_row_id, _last_updated_sequence_number)} pairs on both engines,
     * and against {@code expectedPairs} derived from the Iceberg metadata.
     */
    private void assertLineageMatchesIcebergMetadata(Session session, String tableName, List<long[]> expectedPairs)
    {
        String sql = "SELECT \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName + " ORDER BY \"_row_id\"";
        assertQuery(session, sql, sessionWithPushdown(false), sql);
        List<MaterializedRow> rows = computeActual(session, sql).getMaterializedRows();
        assertEquals(rows.size(), expectedPairs.size(), "Presto and Iceberg API should return the same number of rows");
        for (int i = 0; i < rows.size(); i++) {
            Long rowId = (Long) rows.get(i).getField(0);
            Long seqNum = (Long) rows.get(i).getField(1);
            assertNotNull(rowId, "Presto _row_id should not be null for V3 table");
            assertNotNull(seqNum, "Presto _last_updated_sequence_number should not be null");
            assertEquals(rowId.longValue(), expectedPairs.get(i)[0], "_row_id should match Iceberg metadata");
            assertEquals(seqNum.longValue(), expectedPairs.get(i)[1], "_last_updated_sequence_number should match Iceberg metadata");
        }
    }

    private List<long[]> readIdAndSequenceNumber(Session session, String tableName)
    {
        return readIdAndLongColumn(session, tableName, "_last_updated_sequence_number");
    }

    private List<long[]> readIdAndRowId(Session session, String tableName)
    {
        return readIdAndLongColumn(session, tableName, "_row_id");
    }

    private List<long[]> readIdAndLongColumn(Session session, String tableName, String column)
    {
        String sql = "SELECT id, \"" + column + "\" FROM " + tableName;
        assertQuery(session, sql, sessionWithPushdown(false), sql);
        List<long[]> rows = new ArrayList<>();
        for (MaterializedRow row : computeActual(session, sql).getMaterializedRows()) {
            rows.add(new long[] {(Integer) row.getField(0), (Long) row.getField(1)});
        }
        return rows;
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

    /**
     * Commits ids 1 and 2 in separate appends, then rewrites both into one file in which id 1
     * stores its original lineage and id 2 stores none, so id 2 inherits the new file's
     * {@code firstRowId + 1} and data sequence number.
     */
    private MixedLineageTable createMixedLineageTable(Session session, Catalog catalog, TableIdentifier tableId, String tableName)
            throws Exception
    {
        Table table = createTestTable(catalog, tableId, "3");
        appendOneRow(table, 1, "one");
        appendOneRow(table, 2, "two");
        long storedRowId = valueForId(readIdAndRowId(session, tableName), 1);
        long storedSeq = valueForId(readIdAndSequenceNumber(session, tableName), 1);

        Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
        Record stored = GenericRecord.create(lineageSchema);
        stored.setField("id", 1);
        stored.setField("value", "one");
        stored.setField(MetadataColumns.ROW_ID.name(), storedRowId);
        stored.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), storedSeq);
        Record inherited = GenericRecord.create(lineageSchema);
        inherited.setField("id", 2);
        inherited.setField("value", "two-updated");
        writeMixedLineageFile(table, lineageSchema, stored, inherited);

        DataFile file = getOnlyElement(dataFiles(table));
        MixedLineageTable mixed = new MixedLineageTable(storedRowId, storedSeq, file.firstRowId() + 1, file.dataSequenceNumber());
        assertTrue(mixed.inheritedSeq > mixed.storedSeq, "the rewrite must commit at a newer sequence number");

        List<long[]> idAndRowId = readIdAndRowId(session, tableName);
        assertEquals(valueForId(idAndRowId, 1), mixed.storedRowId);
        assertEquals(valueForId(idAndRowId, 2), mixed.inheritedRowId);
        List<long[]> idAndSeq = readIdAndSequenceNumber(session, tableName);
        assertEquals(valueForId(idAndSeq, 1), mixed.storedSeq);
        assertEquals(valueForId(idAndSeq, 2), mixed.inheritedSeq);
        return mixed;
    }

    private static class MixedLineageTable
    {
        private final long storedRowId;
        private final long storedSeq;
        private final long inheritedRowId;
        private final long inheritedSeq;

        private MixedLineageTable(long storedRowId, long storedSeq, long inheritedRowId, long inheritedSeq)
        {
            this.storedRowId = storedRowId;
            this.storedSeq = storedSeq;
            this.inheritedRowId = inheritedRowId;
            this.inheritedSeq = inheritedSeq;
        }
    }

    private File getCatalogDirectory()
    {
        return IcebergQueryRunner.getIcebergDataDirectoryPath(
                        getDistributedQueryRunner().getCoordinator().getDataDirectory(),
                        CatalogType.HADOOP.name(),
                        new IcebergConfig().getFileFormat(),
                        true)
                .toFile();
    }

    private Catalog loadCatalog()
    {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(),
                ICEBERG_CATALOG,
                ImmutableMap.of("warehouse", getCatalogDirectory().toURI().toString()),
                new Configuration());
    }

    private static Table createTestTable(Catalog catalog, TableIdentifier tableId, String formatVersion)
    {
        return catalog.createTable(tableId, TEST_TABLE_SCHEMA, PartitionSpec.unpartitioned(), ImmutableMap.of("format-version", formatVersion));
    }

    /**
     * Drops a test table during teardown. A failure to drop is ignored so that it can never replace
     * the assertion error that actually failed the test.
     */
    private static void dropTableQuietly(Catalog catalog, TableIdentifier tableId)
    {
        try {
            catalog.dropTable(tableId, true);
        }
        catch (Exception ignored) {
        }
    }

    /**
     * Derives the {@code (_row_id, _last_updated_sequence_number)} pairs the table should report
     * from the Iceberg metadata alone: each row's id is its file's {@code firstRowId} plus its
     * position, and its sequence number is the file's {@code dataSequenceNumber}. Sorted by row id.
     */
    private static List<long[]> buildExpectedPairs(Table table, String firstRowIdMessage)
            throws Exception
    {
        List<long[]> pairs = new ArrayList<>();
        for (DataFile dataFile : dataFiles(table)) {
            Long firstRowId = dataFile.firstRowId();
            assertNotNull(firstRowId, firstRowIdMessage);
            assertTrue(firstRowId >= 0, "firstRowId should be non-negative: " + firstRowId);
            for (long pos = 0; pos < dataFile.recordCount(); pos++) {
                pairs.add(new long[] {firstRowId + pos, dataFile.dataSequenceNumber()});
            }
        }
        pairs.sort((a, b) -> Long.compare(a[0], b[0]));
        return pairs;
    }

    private static List<Integer> sortedIdsOf(MaterializedResult result)
    {
        List<Integer> ids = new ArrayList<>();
        for (MaterializedRow row : result.getMaterializedRows()) {
            ids.add((Integer) row.getField(0));
        }
        ids.sort(Integer::compare);
        return ids;
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

    private static long valueForId(List<long[]> rows, int id)
    {
        for (long[] row : rows) {
            if (row[0] == id) {
                return row[1];
            }
        }
        throw new AssertionError("id not found: " + id);
    }

    private static void appendOneRow(Table table, int id, String value)
            throws Exception
    {
        writeRecords(table, GenericRecord.create(table.schema()).copy("id", id, "value", value));
        table.refresh();
    }

    private static void writeRecords(Table table, Record... records)
            throws Exception
    {
        writeRecordsWithSchema(table, table.schema(), records);
    }

    private static void writeRecordsWithSchema(Table table, Schema writeSchema, Record... records)
            throws Exception
    {
        table.newAppend().appendFile(writeFile(table, writeSchema, records)).commit();
    }

    /**
     * Simulates a row-preserving external UPDATE/MERGE: the new data file explicitly carries
     * {@code _row_id} / {@code _last_updated_sequence_number} values that override the positional
     * and file-level fallbacks.
     */
    private static void writeOverriddenLineageRow(Table table)
            throws Exception
    {
        Schema lineageSchema = MetadataColumns.schemaWithRowLineage(table.schema());
        Record updatedRow = GenericRecord.create(lineageSchema);
        updatedRow.setField("id", 2);
        updatedRow.setField("value", "two-updated");
        updatedRow.setField(MetadataColumns.ROW_ID.name(), 42L);
        updatedRow.setField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), 99L);
        writeRecordsWithSchema(table, lineageSchema, updatedRow);
    }

    /**
     * Replaces every data file in the table with one file holding {@code rows}, as a copy-on-write
     * UPDATE does. Rows that leave {@code _row_id} / {@code _last_updated_sequence_number} null
     * inherit them from the new file, so the file mixes stored and inherited lineage.
     */
    private static void writeMixedLineageFile(Table table, Schema lineageSchema, Record... rows)
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
     * V3 requires deletion vectors for position deletes, which the Java worker does not read, so
     * equality deletes are the delete form both engines can read.
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
        return writeFile(table, writeSchema, MetricsConfig.forTable(table), records);
    }

    private static DataFile writeFile(Table table, Schema writeSchema, MetricsConfig metricsConfig, Record... records)
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
}
