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

import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.Iterables.getOnlyElement;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Iceberg V3 row lineage ({@code _row_id} / {@code _last_updated_sequence_number}) tests, run on
 * native workers and cross-checked against Java workers over the same files.
 * <p>
 * Every lineage query is checked against the Java workers and against expected values taken from
 * the Iceberg metadata or from the values written through the Iceberg API. See
 * {@link AbstractTestIcebergRowLineage} for how the two query runners share one warehouse.
 */
public class TestIcebergV3RowLineage
        extends AbstractTestIcebergRowLineage
{
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
            dropTableQuietly(catalog, tableId);
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

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(distinctRowIdsSql);
            long distinctRowIds = (Long) computeScalar(distinctRowIdsSql);
            assertEquals(distinctRowIds, 3L, "Row IDs must be unique across all rows");
        }
        finally {
            dropTableQuietly(catalog, tableId);
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
            assertMatchesJavaWorkerWithPushdown(nonNullRowIdsSql);
            assertEquals(computeScalar(nonNullRowIdsSql), 0L,
                    "_row_id should be null for all rows in a V2 table");

            String nonNullSeqNumsSql = "SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NOT NULL";
            assertQuery(nonNullSeqNumsSql);
            assertMatchesJavaWorkerWithPushdown(nonNullSeqNumsSql);
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
            assertMatchesJavaWorkerWithPushdown(nullRowIdsSql);
            assertEquals(computeScalar(nullRowIdsSql), 0L,
                    "All rows should have non-null _row_id after V3 upgrade");

            String nullSeqNumsSql = "SELECT count(*) FROM " + tableName + " WHERE \"_last_updated_sequence_number\" IS NULL";
            assertQuery(nullSeqNumsSql);
            assertMatchesJavaWorkerWithPushdown(nullSeqNumsSql);
            assertEquals(computeScalar(nullSeqNumsSql), 0L,
                    "All rows should have non-null _last_updated_sequence_number after V3 upgrade");

            String distinctRowIdsSql = "SELECT count(DISTINCT \"_row_id\") FROM " + tableName;
            assertQuery(distinctRowIdsSql);
            long distinctRowIds = (Long) computeScalar(distinctRowIdsSql);
            assertEquals(distinctRowIds, 5L, "Row IDs must be unique across all 5 rows after upgrade");

            table.refresh();
            List<long[]> allExpectedPairs = buildExpectedPairs(table,
                    "All files should have firstRowId set after V3 upgrade");
            assertPrestoRowLineageMatchesExpected(tableName, allExpectedPairs);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * A row whose {@code _row_id} / {@code _last_updated_sequence_number} are stored in the data
     * file, as an external row-preserving UPDATE/MERGE writes them, reports those stored values
     * whether or not the query also filters on the column. The reader learns the columns' field
     * ids from the projection as well as from the filter.
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

            // Pure insert: relies on the firstRowId + position fallback.
            writeRecords(table, GenericRecord.create(table.schema()).copy("id", 1, "value", "one"));
            table.refresh();

            writeOverriddenLineageRow(table);
            table.refresh();

            String unfilteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                    " ORDER BY id";
            String filteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                    " WHERE \"_row_id\" IS NOT NULL ORDER BY id";
            assertQueryOrdered(unfilteredSql);
            assertQueryOrdered(filteredSql);
            assertMatchesJavaWorkerWithPushdown(filteredSql);

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
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * {@link #testRowLineageConsistentAcrossPredicateAndProjectionOnlyQueries} with filter pushdown
     * enabled. IcebergFilterPushdown alone builds the layout in this mode, and it takes the
     * requested columns from the table scan's output, so the stored values still reach the reader.
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

            writeRecords(table, GenericRecord.create(table.schema()).copy("id", 1, "value", "one"));
            table.refresh();

            writeOverriddenLineageRow(table);
            table.refresh();

            String unfilteredSql = "SELECT id, \"_row_id\", \"_last_updated_sequence_number\" FROM " + tableName +
                    " ORDER BY id";
            assertMatchesJavaWorkerWithPushdown(unfilteredSql);

            MaterializedResult unfiltered = computeActual(pushdownFilterSession(), unfilteredSql);

            assertEquals(unfiltered.getRowCount(), 2);
            Map<Integer, long[]> unfilteredById = rowIdAndSeqById(unfiltered);
            assertEquals(unfilteredById.get(2)[0], 42L,
                    "_row_id should reflect the file's explicit physical value under pushdown filter too, not firstRowId+position");
            assertEquals(unfilteredById.get(2)[1], 99L,
                    "_last_updated_sequence_number should reflect the file's explicit physical value under pushdown filter too");
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test
    public void testPredicatePushdownPreCompaction()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_pre";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            List<long[]> idAndSeq = readIdAndSequenceNumber(tableName);
            assertEquals(idAndSeq.size(), 3);
            long seq1 = valueForId(idAndSeq, 1);
            long seq2 = valueForId(idAndSeq, 2);
            long seq3 = valueForId(idAndSeq, 3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            assertIdsForPredicate(tableName, "<= " + seq1, ImmutableList.of(1));
            assertIdsForPredicate(tableName, "<= " + seq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(tableName, "<= " + seq3, ImmutableList.of(1, 2, 3));
            assertIdsForPredicate(tableName, "< " + seq1, ImmutableList.of());
            assertIdsForPredicate(tableName, "BETWEEN " + seq2 + " AND " + seq3, ImmutableList.of(2, 3));
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
    @Test
    public void testPredicatePushdownPostCompaction()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_post";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            table.refresh();
            List<long[]> preIdAndSeq = readIdAndSequenceNumber(tableName);
            long preSeq1 = valueForId(preIdAndSeq, 1);
            long preSeq2 = valueForId(preIdAndSeq, 2);
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
            DataFile compactedFile = writeFile(table, lineageAugmentedSchema, row1, row2);

            Set<DataFile> compactedFiles = new HashSet<>();
            compactedFiles.add(compactedFile);
            table.newRewrite()
                    .rewriteFiles(preCompactionFiles, compactedFiles)
                    .commit();

            List<long[]> postIdAndSeq = readIdAndSequenceNumber(tableName);
            assertEquals(postIdAndSeq.size(), 2);
            assertEquals(valueForId(postIdAndSeq, 1), preSeq1);
            assertEquals(valueForId(postIdAndSeq, 2), preSeq2);

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
            dropTableQuietly(catalog, tableId);
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
            Table table = createTestTable(catalog, tableId, "2");
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");

            assertIdsForPredicate(tableName, "<= 100", ImmutableList.of());
            assertIdsForPredicate(tableName, "> 0", ImmutableList.of());
            assertIdsForPredicate(tableName, "IS NOT NULL", ImmutableList.of());
            assertIdsForPredicate(tableName, "IS NULL", ImmutableList.of(1, 2));
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
    @Test
    public void testPredicateActuallyPrunesSplits()
            throws Exception
    {
        String tableName = "test_lineage_pushdown_split_count";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            long minSeq = valueForId(readIdAndSequenceNumber(tableName), 1);

            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsPruned = completedSplitsFor(
                    "SELECT id FROM " + tableName +
                            " WHERE \"_last_updated_sequence_number\" < " + minSeq);

            assertTrue(splitsAll > splitsPruned,
                    "expected predicate to prune splits but unrestricted=" + splitsAll
                            + " pruned=" + splitsPruned);
        }
        finally {
            dropTableQuietly(catalog, tableId);
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
            Table table = createTestTable(catalog, tableId, "3");

            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");

            List<long[]> idAndSeq = readIdAndSequenceNumber(tableName);
            long seq1 = valueForId(idAndSeq, 1);
            long seq2 = valueForId(idAndSeq, 2);
            long seq3 = valueForId(idAndSeq, 3);
            assertTrue(seq1 < seq2 && seq2 < seq3, "sequence numbers must increase per commit");

            String disjointPredicate = " WHERE \"_last_updated_sequence_number\" <= " + seq1
                    + " OR \"_last_updated_sequence_number\" >= " + seq3;

            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsDisjoint = completedSplitsFor("SELECT id FROM " + tableName + disjointPredicate);
            assertTrue(splitsAll > splitsDisjoint,
                    "expected disjoint OR to prune middle file but unrestricted=" + splitsAll
                            + " disjoint=" + splitsDisjoint);

            String disjointSql = "SELECT id FROM " + tableName + disjointPredicate + " ORDER BY id";
            assertIdsForQuery(disjointSql, ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test
    public void testRowIdPredicatesOnInheritedValues()
            throws Exception
    {
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
            List<long[]> idAndRowId = readIdAndRowId(tableName);
            assertEquals(valueForId(idAndRowId, 1), rowId1);
            assertEquals(valueForId(idAndRowId, 2), rowId2);
            assertEquals(valueForId(idAndRowId, 3), rowId3);

            assertIdsForPredicate(tableName, "_row_id", "= " + rowId2, ImmutableList.of(2));
            assertIdsForPredicate(tableName, "_row_id", "< " + rowId3, ImmutableList.of(1, 2));
            assertIdsForPredicate(tableName, "_row_id", "IN (" + rowId1 + ", " + rowId3 + ")", ImmutableList.of(1, 3));
            assertIdsForPredicate(tableName, "_row_id", "> " + rowId3, ImmutableList.of());
            assertIdsForQuery("SELECT id FROM " + tableName +
                            " WHERE \"_row_id\" = " + rowId1 + " OR \"_row_id\" = " + rowId3 + " ORDER BY id",
                    ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test
    public void testRowIdPredicatesOnStoredValues()
            throws Exception
    {
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

            assertIdsForPredicate(tableName, "_row_id", "= 42", ImmutableList.of(2));
            assertIdsForPredicate(tableName, "_row_id", "= " + inheritedRowId, ImmutableList.of());
            assertIdsForPredicate(tableName, "_last_updated_sequence_number", "= 99", ImmutableList.of(2));
            assertIdsForPredicate(tableName, "_last_updated_sequence_number", "= " + inheritedSeq, ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test
    public void testPredicatesOnMixedStoredAndInheritedFile()
            throws Exception
    {
        String tableName = "test_lineage_mixed_file";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            MixedLineageTable mixed = createMixedLineageTable(catalog, tableId, tableName);

            assertIdsForPredicate(tableName, "_row_id", "= " + mixed.inheritedRowId, ImmutableList.of(2));
            assertIdsForPredicate(tableName, "_row_id", "= " + mixed.storedRowId, ImmutableList.of(1));
            assertIdsForPredicate(tableName, "_last_updated_sequence_number", "= " + mixed.inheritedSeq, ImmutableList.of(2));
            assertIdsForPredicate(tableName, "_last_updated_sequence_number", ">= " + mixed.inheritedSeq, ImmutableList.of(2));
            assertIdsForPredicate(tableName, "_last_updated_sequence_number", "= " + mixed.storedSeq, ImmutableList.of(1));
        }
        finally {
            dropTableQuietly(catalog, tableId);
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
            Table inherited = createTestTable(catalog, inheritedTableId, "3");
            appendOneRow(inherited, 1, "one");
            appendOneRow(inherited, 2, "two");
            createMixedLineageTable(catalog, mixedTableId, mixedTableName);

            for (String tableName : ImmutableList.of(inheritedTableName, mixedTableName)) {
                for (String column : ImmutableList.of("_row_id", "_last_updated_sequence_number")) {
                    assertIdsForPredicate(tableName, column, "IS NULL", ImmutableList.of());
                    assertIdsForPredicate(tableName, column, "IS NOT NULL", ImmutableList.of(1, 2));
                }
            }
        }
        finally {
            dropTableQuietly(catalog, inheritedTableId);
            dropTableQuietly(catalog, mixedTableId);
        }
    }

    @Test
    public void testLineagePredicateWithDeletedRow()
            throws Exception
    {
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

            List<long[]> idAndRowId = readIdAndRowId(tableName);
            assertEquals(idAndRowId.size(), 2);
            assertEquals(valueForId(idAndRowId, 1), firstRowId);
            assertEquals(valueForId(idAndRowId, 3), firstRowId + 2);

            assertIdsForPredicate(tableName, "_row_id", "= " + (firstRowId + 2), ImmutableList.of(3));
            assertIdsForPredicate(tableName, "_row_id", "= " + (firstRowId + 1), ImmutableList.of());
            assertIdsForPredicate(tableName, "_last_updated_sequence_number", "= " + file.dataSequenceNumber(), ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test
    public void testLineagePredicateCombinedWithRegularColumn()
            throws Exception
    {
        String tableName = "test_lineage_combined_predicates";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            appendOneRow(table, 3, "three");
            List<long[]> idAndRowId = readIdAndRowId(tableName);
            long rowId1 = valueForId(idAndRowId, 1);
            long seq2 = valueForId(readIdAndSequenceNumber(tableName), 2);
            ImmutableList.Builder<Integer> evenRowIds = ImmutableList.builder();
            for (long[] row : idAndRowId) {
                if (row[1] % 2 == 0) {
                    evenRowIds.add((int) row[0]);
                }
            }

            assertIdsForQuery("SELECT id FROM " + tableName +
                            " WHERE \"_last_updated_sequence_number\" >= " + seq2 + " AND value <> 'three' ORDER BY id",
                    ImmutableList.of(2));
            assertIdsForQuery("SELECT id FROM " + tableName +
                            " WHERE \"_row_id\" = " + rowId1 + " OR id = 3 ORDER BY id",
                    ImmutableList.of(1, 3));
            assertIdsForQuery("SELECT id FROM " + tableName + " WHERE \"_row_id\" % 2 = 0 ORDER BY id",
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
    @Test
    public void testCountsOnlyCompactedFileIsNotMisPruned()
            throws Exception
    {
        String tableName = "test_lineage_counts_only";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            Table table = createTestTable(catalog, tableId, "3");
            appendOneRow(table, 1, "one");
            appendOneRow(table, 2, "two");
            List<long[]> idAndRowId = readIdAndRowId(tableName);
            List<long[]> idAndSeq = readIdAndSequenceNumber(tableName);
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

            assertIdsForPredicate(tableName, "= " + preSeq1, ImmutableList.of(1));
            assertIdsForPredicate(tableName, "= " + preSeq2, ImmutableList.of(2));
            assertIdsForPredicate(tableName, "<= " + preSeq2, ImmutableList.of(1, 2));
            assertIdsForPredicate(tableName, "= " + committed.dataSequenceNumber(), ImmutableList.of());
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    @Test
    public void testMixedFileStillPrunedWhenNothingMatches()
            throws Exception
    {
        String tableName = "test_lineage_mixed_file_pruned";
        Catalog catalog = loadCatalog();
        TableIdentifier tableId = TableIdentifier.of(TEST_SCHEMA, tableName);
        try {
            MixedLineageTable mixed = createMixedLineageTable(catalog, tableId, tableName);
            Table table = catalog.loadTable(tableId);
            appendOneRow(table, 3, "three");
            long seq3 = valueForId(readIdAndSequenceNumber(tableName), 3);
            assertTrue(seq3 > mixed.inheritedSeq);

            String selective = "SELECT id FROM " + tableName + " WHERE \"_last_updated_sequence_number\" = " + seq3;
            assertIdsForQuery(selective + " ORDER BY id", ImmutableList.of(3));
            int splitsAll = completedSplitsFor("SELECT id FROM " + tableName);
            int splitsSelective = completedSplitsFor(selective);
            assertTrue(splitsAll > splitsSelective,
                    "expected the mixed file to be pruned but unrestricted=" + splitsAll + " selective=" + splitsSelective);
        }
        finally {
            dropTableQuietly(catalog, tableId);
        }
    }

    /**
     * Commits ids 1 and 2 in separate appends, then rewrites both into one file in which id 1
     * stores its original lineage and id 2 stores none, so id 2 inherits the new file's
     * {@code firstRowId + 1} and data sequence number.
     */
    private MixedLineageTable createMixedLineageTable(Catalog catalog, TableIdentifier tableId, String tableName)
            throws Exception
    {
        Table table = createTestTable(catalog, tableId, "3");
        appendOneRow(table, 1, "one");
        appendOneRow(table, 2, "two");
        long storedRowId = valueForId(readIdAndRowId(tableName), 1);
        long storedSeq = valueForId(readIdAndSequenceNumber(tableName), 1);

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

        List<long[]> idAndRowId = readIdAndRowId(tableName);
        assertEquals(valueForId(idAndRowId, 1), mixed.storedRowId);
        assertEquals(valueForId(idAndRowId, 2), mixed.inheritedRowId);
        List<long[]> idAndSeq = readIdAndSequenceNumber(tableName);
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

    /**
     * Simulates a row-preserving external UPDATE/MERGE (e.g. a real Spark MERGE INTO): the new data
     * file explicitly carries {@code _row_id} / {@code _last_updated_sequence_number} values that
     * must override the positional and file-level fallbacks.
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
}
