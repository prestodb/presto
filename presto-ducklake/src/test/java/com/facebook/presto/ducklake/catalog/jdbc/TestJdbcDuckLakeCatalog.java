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
package com.facebook.presto.ducklake.catalog.jdbc;

import com.facebook.presto.ducklake.TestingDuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeColumnRow;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeFileColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedTable;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.ducklake.catalog.DuckLakeSchema;
import com.facebook.presto.ducklake.catalog.DuckLakeSnapshot;
import com.facebook.presto.ducklake.catalog.DuckLakeTable;
import com.facebook.presto.ducklake.catalog.DuckLakeTableColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakeTableStats;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJdbcDuckLakeCatalog
{
    private TestingDuckLakeCatalog testingCatalog;
    private JdbcDuckLakeCatalog catalog;

    @BeforeClass
    public void setUp()
    {
        testingCatalog = new TestingDuckLakeCatalog();
        catalog = testingCatalog.createCatalog();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (testingCatalog != null) {
            testingCatalog.close();
        }
    }

    @Test
    public void testLatestSnapshotIsMaxId()
    {
        List<DuckLakeSnapshot> snapshots = catalog.listSnapshots();
        long expectedLatest = snapshots.stream()
                .mapToLong(DuckLakeSnapshot::getSnapshotId)
                .max()
                .orElseThrow(AssertionError::new);
        assertEquals(catalog.getLatestSnapshotId(), expectedLatest);
    }

    @Test
    public void testGetSnapshotByBadIdIsEmpty()
    {
        assertEquals(catalog.getSnapshot(Long.MAX_VALUE), Optional.empty());
    }

    @Test
    public void testListSnapshotsOrderedWithDetails()
    {
        List<DuckLakeSnapshot> snapshots = catalog.listSnapshots();
        for (int i = 1; i < snapshots.size(); i++) {
            assertTrue(snapshots.get(i - 1).getSnapshotId() < snapshots.get(i).getSnapshotId());
        }

        DuckLakeSnapshot snapshotTwo = catalog.getSnapshot(2).orElseThrow(AssertionError::new);
        assertEquals(snapshotTwo.getAuthor(), Optional.of("fixture-generator"));
        assertEquals(snapshotTwo.getCommitMessage(), Optional.of("Load TPC-H tiny fixture (nation, region, customer, orders)"));
    }

    @Test
    public void testSchemasAtLatestSnapshot()
    {
        long latest = catalog.getLatestSnapshotId();
        List<String> schemaNames = catalog.listSchemas(latest).stream()
                .map(DuckLakeSchema::getSchemaName)
                .collect(Collectors.toList());
        assertTrue(schemaNames.containsAll(List.of("main", "tpch", "types", "part", "del", "evo", "inl", "merge")));
    }

    @Test
    public void testTablesInTpch()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema tpch = catalog.getSchema(latest, "tpch").orElseThrow(AssertionError::new);
        List<String> tableNames = catalog.listTables(latest, tpch).stream()
                .map(DuckLakeTable::getTableName)
                .collect(Collectors.toList());
        assertEquals(tableNames, List.of("customer", "nation", "orders", "region"));
    }

    @Test
    public void testTpchTablePathIsResolved()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema tpch = catalog.getSchema(latest, "tpch").orElseThrow(AssertionError::new);
        DuckLakeTable customer = catalog.getTable(latest, tpch, "customer").orElseThrow(AssertionError::new);
        assertEquals(customer.getPath(), testingCatalog.getDataPath() + "tpch/customer/");
    }

    @Test
    public void testNestedColumnsHaveParents()
    {
        long latest = catalog.getLatestSnapshotId();
        List<DuckLakeColumnRow> columns = catalog.listColumns(latest, 8L);

        DuckLakeColumnRow listColumn = findColumn(columns, "list_col");
        DuckLakeColumnRow element = findColumn(columns, "element");
        assertEquals(element.getParentColumn(), OptionalLong.of(listColumn.getColumnId()));

        DuckLakeColumnRow structColumn = findColumn(columns, "struct_col");
        assertEquals(findColumn(columns, "a").getParentColumn(), OptionalLong.of(structColumn.getColumnId()));
        assertEquals(findColumn(columns, "b").getParentColumn(), OptionalLong.of(structColumn.getColumnId()));

        DuckLakeColumnRow mapColumn = findColumn(columns, "map_col");
        assertEquals(findColumn(columns, "key").getParentColumn(), OptionalLong.of(mapColumn.getColumnId()));
        assertEquals(findColumn(columns, "value").getParentColumn(), OptionalLong.of(mapColumn.getColumnId()));

        List<Long> columnOrders = columns.stream().map(DuckLakeColumnRow::getColumnOrder).collect(Collectors.toList());
        List<Long> sortedColumnOrders = columnOrders.stream().sorted().collect(Collectors.toList());
        assertEquals(columnOrders, sortedColumnOrders);
    }

    @Test
    public void testSnapshotOneSeesMainAndEmptyTpch()
    {
        List<String> schemaNames = catalog.listSchemas(1).stream()
                .map(DuckLakeSchema::getSchemaName)
                .sorted()
                .collect(Collectors.toList());
        assertEquals(schemaNames, List.of("main", "tpch"));

        DuckLakeSchema tpch = catalog.getSchema(1, "tpch").orElseThrow(AssertionError::new);
        assertTrue(catalog.listTables(1, tpch).isEmpty());
    }

    @Test
    public void testSnapshotZeroSeesOnlyMain()
    {
        List<String> schemaNames = catalog.listSchemas(0).stream()
                .map(DuckLakeSchema::getSchemaName)
                .collect(Collectors.toList());
        assertEquals(schemaNames, List.of("main"));
    }

    @Test
    public void testEvoTableColumnRenameIsVersioned()
            throws SQLException
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema evo = catalog.getSchema(latest, "evo").orElseThrow(AssertionError::new);
        DuckLakeTable table = catalog.getTable(latest, evo, "table").orElseThrow(AssertionError::new);

        List<String> latestColumnNames = catalog.listColumns(latest, table.getTableId()).stream()
                .map(DuckLakeColumnRow::getColumnName)
                .collect(Collectors.toList());
        assertEquals(latestColumnNames, List.of("id", "full_name", "score"));

        long creationSnapshot = getBeginSnapshot(table.getTableId());
        List<String> creationColumnNames = catalog.listColumns(creationSnapshot, table.getTableId()).stream()
                .map(DuckLakeColumnRow::getColumnName)
                .collect(Collectors.toList());
        assertEquals(creationColumnNames, List.of("id", "name"));
    }

    @Test
    public void testGetSnapshotAtOrBefore()
    {
        List<DuckLakeSnapshot> snapshots = catalog.listSnapshots();
        DuckLakeSnapshot latestSnapshot = snapshots.get(snapshots.size() - 1);
        DuckLakeSnapshot firstSnapshot = snapshots.get(0);

        Optional<DuckLakeSnapshot> atLatestTime = catalog.getSnapshotAtOrBefore(latestSnapshot.getSnapshotTime());
        assertTrue(atLatestTime.isPresent());
        assertEquals(atLatestTime.get().getSnapshotId(), latestSnapshot.getSnapshotId());

        Instant beforeFirstSnapshot = firstSnapshot.getSnapshotTime().minus(1, ChronoUnit.DAYS);
        assertEquals(catalog.getSnapshotAtOrBefore(beforeFirstSnapshot), Optional.empty());
    }

    @Test
    public void testDelSimpleDataFileHasDeleteFile()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema del = catalog.getSchema(latest, "del").orElseThrow(AssertionError::new);
        DuckLakeTable simple = catalog.getTable(latest, del, "simple").orElseThrow(AssertionError::new);

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, simple);
        assertEquals(dataFiles.size(), 1);

        DuckLakeDataFile dataFile = dataFiles.get(0);
        assertEquals(dataFile.getRecordCount(), 1000);
        assertTrue(Files.exists(Paths.get(dataFile.getPath())));

        assertTrue(dataFile.getDeleteFile().isPresent());
        assertEquals(dataFile.getDeleteFile().get().getDeleteCount(), 100);
        assertEquals(dataFile.getDeleteFile().get().getPartialMax(), OptionalLong.empty());
        assertTrue(Files.exists(Paths.get(dataFile.getDeleteFile().get().getPath())));
    }

    @Test
    public void testDelTwoSnapshotsDeleteFileStats()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema del = catalog.getSchema(latest, "del").orElseThrow(AssertionError::new);
        DuckLakeTable twoSnapshots = catalog.getTable(latest, del, "two_snapshots").orElseThrow(AssertionError::new);

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, twoSnapshots);
        assertEquals(dataFiles.size(), 1);
        assertTrue(dataFiles.get(0).getDeleteFile().isPresent());
        assertEquals(dataFiles.get(0).getDeleteFile().get().getDeleteCount(), 200);
        assertEquals(dataFiles.get(0).getDeleteFile().get().getPartialMax(), OptionalLong.of(25));
    }

    @Test
    public void testPartByMonthPartitionFieldsAndValues()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema part = catalog.getSchema(latest, "part").orElseThrow(AssertionError::new);
        DuckLakeTable byMonth = catalog.getTable(latest, part, "by_month").orElseThrow(AssertionError::new);

        List<DuckLakePartitionField> partitionFields = catalog.listPartitionFields(latest, byMonth.getTableId());
        assertEquals(partitionFields.size(), 2);
        assertEquals(partitionFields.get(0).getPartitionKeyIndex(), 0);
        assertEquals(partitionFields.get(0).getTransform(), "month");
        assertEquals(partitionFields.get(1).getPartitionKeyIndex(), 1);
        assertEquals(partitionFields.get(1).getTransform(), "year");

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, byMonth);
        assertEquals(dataFiles.size(), 14);
        for (DuckLakeDataFile dataFile : dataFiles) {
            assertEquals(dataFile.getPartitionValues().size(), 2);
            assertTrue(dataFile.getPartitionValues().get(0).isPresent());
            assertTrue(dataFile.getPartitionValues().get(1).isPresent());
        }
    }

    @Test
    public void testPartByIdentityPartitionValuePerFile()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema part = catalog.getSchema(latest, "part").orElseThrow(AssertionError::new);
        DuckLakeTable byIdentity = catalog.getTable(latest, part, "by_identity").orElseThrow(AssertionError::new);

        List<DuckLakePartitionField> partitionFields = catalog.listPartitionFields(latest, byIdentity.getTableId());
        assertEquals(partitionFields.size(), 1);
        assertEquals(partitionFields.get(0).getColumnId(), 1);
        assertEquals(partitionFields.get(0).getTransform(), "identity");

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, byIdentity);
        assertEquals(dataFiles.size(), 5);
        for (DuckLakeDataFile dataFile : dataFiles) {
            assertEquals(dataFile.getPartitionValues().size(), 1);
        }
    }

    @Test
    public void testTpchNationIsUnpartitioned()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema tpch = catalog.getSchema(latest, "tpch").orElseThrow(AssertionError::new);
        DuckLakeTable nation = catalog.getTable(latest, tpch, "nation").orElseThrow(AssertionError::new);
        assertTrue(catalog.listPartitionFields(latest, nation.getTableId()).isEmpty());
    }

    @Test
    public void testEvoTableFilesHaveDifferentStatsColumns()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema evo = catalog.getSchema(latest, "evo").orElseThrow(AssertionError::new);
        DuckLakeTable table = catalog.getTable(latest, evo, "table").orElseThrow(AssertionError::new);

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, table);
        assertEquals(dataFiles.size(), 2);
        assertEquals(dataFiles.get(0).getColumnStats().keySet(), ImmutableSet.of(1L, 2L));
        assertEquals(dataFiles.get(1).getColumnStats().keySet(), ImmutableSet.of(1L, 2L, 3L));
    }

    @Test
    public void testInlSmallHasOneInlinedTable()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema inl = catalog.getSchema(latest, "inl").orElseThrow(AssertionError::new);
        DuckLakeTable small = catalog.getTable(latest, inl, "small").orElseThrow(AssertionError::new);

        List<DuckLakeInlinedTable> inlinedTables = catalog.listInlinedTables(small.getTableId());
        assertEquals(inlinedTables.size(), 1);
        assertEquals(inlinedTables.get(0).getTableName(), "ducklake_inlined_data_21_21");
        assertEquals(inlinedTables.get(0).getSchemaVersion(), 21);
    }

    @Test
    public void testTpchOrdersHasNoInlinedTables()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema tpch = catalog.getSchema(latest, "tpch").orElseThrow(AssertionError::new);
        DuckLakeTable orders = catalog.getTable(latest, tpch, "orders").orElseThrow(AssertionError::new);
        assertTrue(catalog.listInlinedTables(orders.getTableId()).isEmpty());
    }

    @Test
    public void testInlSmallInlinedRowsAtDifferentSnapshots()
            throws SQLException
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema inl = catalog.getSchema(latest, "inl").orElseThrow(AssertionError::new);
        DuckLakeTable small = catalog.getTable(latest, inl, "small").orElseThrow(AssertionError::new);
        DuckLakeInlinedTable inlinedTable = catalog.listInlinedTables(small.getTableId()).get(0);

        assertEquals(readInlinedIds(inlinedTable, latest), List.of(1, 2, 3, 4, 5, 6));

        long creationSnapshot = getBeginSnapshot(small.getTableId());
        assertEquals(readInlinedIds(inlinedTable, creationSnapshot), List.of());

        assertEquals(readInlinedIds(inlinedTable, 34).size(), 1);
    }

    private List<Integer> readInlinedIds(DuckLakeInlinedTable inlinedTable, long snapshotId)
    {
        ImmutableList.Builder<Integer> ids = ImmutableList.builder();
        try (DuckLakeInlinedRowSource rowSource = catalog.openInlinedRows(snapshotId, inlinedTable, List.of("id", "val"))) {
            while (rowSource.advanceNextRow()) {
                ids.add((Integer) rowSource.getObject(0));
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return ids.build();
    }

    @Test
    public void testMergeTableFileHasPartialMax()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema merge = catalog.getSchema(latest, "merge").orElseThrow(AssertionError::new);
        DuckLakeTable table = catalog.getTable(latest, merge, "table").orElseThrow(AssertionError::new);

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, table);
        assertEquals(dataFiles.size(), 1);
        assertEquals(dataFiles.get(0).getRecordCount(), 5);
        assertEquals(dataFiles.get(0).getPartialMax(), OptionalLong.of(44));
    }

    @Test
    public void testDelTwoSnapshotsHasNoFilesBeforeInsertAndFullStatsAtLatest()
            throws SQLException
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema del = catalog.getSchema(latest, "del").orElseThrow(AssertionError::new);
        DuckLakeTable twoSnapshots = catalog.getTable(latest, del, "two_snapshots").orElseThrow(AssertionError::new);

        // The data file backing this table was inserted after the table's creation snapshot, so
        // listDataFiles at (or before) creation must not see it -- and, because the per-file
        // partition-value/column-stats queries are scoped the same way as the data-file query
        // itself (rather than pulling the table's entire file history), this stays cheap rather
        // than merely happening to filter correctly downstream.
        long creationSnapshot = getBeginSnapshot(twoSnapshots.getTableId());
        assertTrue(catalog.listDataFiles(creationSnapshot, twoSnapshots).isEmpty());

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, twoSnapshots);
        assertFalse(dataFiles.isEmpty());
        for (DuckLakeDataFile dataFile : dataFiles) {
            assertFalse(dataFile.getColumnStats().isEmpty());
        }
    }

    @Test
    public void testMergeTableCompactedFileHasNoStaleStats()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema merge = catalog.getSchema(latest, "merge").orElseThrow(AssertionError::new);
        DuckLakeTable table = catalog.getTable(latest, merge, "table").orElseThrow(AssertionError::new);

        // merge.table's pre-compaction files were replaced by ducklake_merge_adjacent_files and
        // are gone from ducklake_data_file entirely; the one surviving (partial) file must still
        // report its own column stats, scoped by the same table_id/snapshot join used for the
        // data-file query rather than a full, unscoped table history scan.
        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, table);
        assertEquals(dataFiles.size(), 1);
        assertFalse(dataFiles.get(0).getColumnStats().isEmpty());
    }

    @Test
    public void testTpchOrdersFileColumnStatsAndTableStats()
    {
        long latest = catalog.getLatestSnapshotId();
        DuckLakeSchema tpch = catalog.getSchema(latest, "tpch").orElseThrow(AssertionError::new);
        DuckLakeTable orders = catalog.getTable(latest, tpch, "orders").orElseThrow(AssertionError::new);

        List<DuckLakeDataFile> dataFiles = catalog.listDataFiles(latest, orders);
        assertEquals(dataFiles.size(), 1);
        assertEquals(dataFiles.get(0).getPath(), testingCatalog.getDataPath() + "tpch/orders/" + "ducklake-01a0880c-56b2-7db0-b6a6-604ddc15d60c.parquet");
        assertTrue(Files.exists(Paths.get(dataFiles.get(0).getPath())));

        DuckLakeColumnRow orderKeyColumn = findColumn(catalog.listColumns(latest, orders.getTableId()), "o_orderkey");
        DuckLakeFileColumnStats orderKeyStats = dataFiles.get(0).getColumnStats().get(orderKeyColumn.getColumnId());
        Long.parseLong(orderKeyStats.getMinValue().orElseThrow(AssertionError::new));
        Long.parseLong(orderKeyStats.getMaxValue().orElseThrow(AssertionError::new));
        assertEquals(orderKeyStats.getNullCount(), OptionalLong.of(0));

        DuckLakeTableStats tableStats = catalog.getTableStats(orders.getTableId()).orElseThrow(AssertionError::new);
        assertEquals(tableStats.getRecordCount(), 15000);

        List<DuckLakeTableColumnStats> tableColumnStats = catalog.listTableColumnStats(orders.getTableId());
        assertFalse(tableColumnStats.isEmpty());
        DuckLakeTableColumnStats orderKeyTableStats = tableColumnStats.stream()
                .filter(stats -> stats.getColumnId() == orderKeyColumn.getColumnId())
                .findFirst()
                .orElseThrow(AssertionError::new);
        assertEquals(orderKeyTableStats.getContainsNull(), Optional.of(false));
    }

    private long getBeginSnapshot(long tableId)
            throws SQLException
    {
        try (Connection connection = testingCatalog.openConnection();
                PreparedStatement statement = connection.prepareStatement("SELECT begin_snapshot FROM public.ducklake_table WHERE table_id = ?")) {
            statement.setLong(1, tableId);
            try (ResultSet resultSet = statement.executeQuery()) {
                assertTrue(resultSet.next());
                return resultSet.getLong("begin_snapshot");
            }
        }
    }

    private static DuckLakeColumnRow findColumn(List<DuckLakeColumnRow> columns, String columnName)
    {
        return columns.stream()
                .filter(column -> column.getColumnName().equals(columnName))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No column named " + columnName));
    }
}
