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
import com.facebook.presto.iceberg.AbstractTestIcebergRowLineage;
import com.facebook.presto.iceberg.CatalogType;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergQueryRunner;
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.CloseableIterable;
import org.testng.annotations.Test;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static com.facebook.presto.iceberg.IcebergSessionProperties.PUSHDOWN_FILTER_ENABLED;
import static com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils.ICEBERG_DEFAULT_STORAGE_FORMAT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Iceberg V3 row lineage ({@code _row_id} / {@code _last_updated_sequence_number}) tests for the
 * native engine.
 * <p>
 * Every lineage query is checked twice: once against the Java engine and once against the Iceberg
 * metadata. The first comparison comes from overriding
 * {@link #assertMatchesReferenceEngine(String)} with {@code assertQueryOrdered}, which runs the
 * query on both the native engine under test and the expected query runner -- both builders resolve
 * to the same data directory and HADOOP catalog, so the two engines read one shared warehouse. The
 * second comes from {@code buildExpectedPairs}, which derives the answer from the file layout.
 * <p>
 * Tables are created and written through the Iceberg API rather than through Presto because row
 * lineage is assigned by the writer, and these cases -- multiple commits, a V2 to V3 upgrade,
 * compaction, and physically materialized lineage columns -- cannot be produced with Presto DML
 * alone.
 */
public class TestIcebergV3RowLineage
        extends AbstractTestIcebergRowLineage
{
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

    @Override
    protected File getCatalogDirectory()
    {
        Path dataDirectory = getDistributedQueryRunner().getCoordinator().getDataDirectory();
        Path catalogDirectory = IcebergQueryRunner.getIcebergDataDirectoryPath(
                dataDirectory, CatalogType.HADOOP.name(),
                new IcebergConfig().getFileFormat(), true);
        return catalogDirectory.toFile();
    }

    /**
     * The expected query runner is the Java engine over the same warehouse, so every inherited
     * helper that routes through this hook becomes a native-vs-Java comparison.
     */
    @Override
    protected void assertMatchesReferenceEngine(String sql)
    {
        assertQueryOrdered(sql);
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
            assertPrestoRowLineageMatchesExpected(tableName, allExpectedPairs);
        }
        finally {
            dropTableQuietly(catalog, tableId);
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

            writeRecords(table, GenericRecord.create(table.schema()).copy("id", 1, "value", "one"));
            table.refresh();

            writeOverriddenLineageRow(table);
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
            long preSeq1 = sequenceNumberForId(preIdAndSeq, 1);
            long preSeq2 = sequenceNumberForId(preIdAndSeq, 2);
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

            String disjointSql = "SELECT id FROM " + tableName + disjointPredicate + " ORDER BY id";
            assertQueryOrdered(disjointSql);
            assertEquals(idsOf(computeActual(disjointSql)), ImmutableList.of(1, 3));
        }
        finally {
            dropTableQuietly(catalog, tableId);
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
