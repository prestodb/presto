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
package com.facebook.presto.ducklake.split;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.ducklake.DuckLakeColumnHandle;
import com.facebook.presto.ducklake.DuckLakeColumnIdentity;
import com.facebook.presto.ducklake.DuckLakeConfig;
import com.facebook.presto.ducklake.DuckLakeSessionProperties;
import com.facebook.presto.ducklake.DuckLakeTableHandle;
import com.facebook.presto.ducklake.DuckLakeTableLayoutHandle;
import com.facebook.presto.ducklake.DuckLakeTableName;
import com.facebook.presto.ducklake.PrestoDuckLakeSchema;
import com.facebook.presto.ducklake.catalog.DuckLakeCatalog;
import com.facebook.presto.ducklake.catalog.DuckLakeColumnRow;
import com.facebook.presto.ducklake.catalog.DuckLakeDataFile;
import com.facebook.presto.ducklake.catalog.DuckLakeDeleteFile;
import com.facebook.presto.ducklake.catalog.DuckLakeFileColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedRowSource;
import com.facebook.presto.ducklake.catalog.DuckLakeInlinedTable;
import com.facebook.presto.ducklake.catalog.DuckLakePartitionField;
import com.facebook.presto.ducklake.catalog.DuckLakeSchema;
import com.facebook.presto.ducklake.catalog.DuckLakeSnapshot;
import com.facebook.presto.ducklake.catalog.DuckLakeTable;
import com.facebook.presto.ducklake.catalog.DuckLakeTableColumnStats;
import com.facebook.presto.ducklake.catalog.DuckLakeTableStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.ducklake.DuckLakeColumnIdentity.TypeCategory.PRIMITIVE;
import static com.facebook.presto.ducklake.DuckLakeErrorCode.DUCKLAKE_UNSUPPORTED_FEATURE;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestDuckLakeSplitSource
{
    private static final DuckLakeTable TABLE = new DuckLakeTable(1L, 2L, "orders", "/data/orders");

    @Test
    public void testDataFileWithDeleteProducesParquetSplit()
    {
        DuckLakeDeleteFile deleteFile = deleteFile("/data/orders/deletes/d1.parquet", "parquet", 25L, 44L, OptionalLong.of(25));
        DuckLakeDataFile dataFile = dataFile(
                "/data/orders/f1.parquet",
                "parquet",
                1024L,
                OptionalLong.of(6),
                OptionalLong.of(44),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(deleteFile),
                ImmutableMap.of(0, Optional.of("2026-09"), 1, Optional.empty()));

        List<ConnectorSplit> splits = getSplits(catalog(ImmutableList.of(dataFile), ImmutableList.of()));

        assertEquals(splits.size(), 1);
        DuckLakeSplit split = (DuckLakeSplit) splits.get(0);
        assertEquals(split.getKind(), DuckLakeSplitKind.PARQUET);
        assertEquals(split.getPath(), "/data/orders/f1.parquet");
        assertEquals(split.getRowIdStart(), OptionalLong.of(6));
        assertEquals(split.getPartialMax(), OptionalLong.of(44));
        assertEquals(split.getPartitionKeys(), ImmutableMap.of(0, Optional.of("2026-09"), 1, Optional.empty()));
        assertEquals(split.getDeletes().size(), 1);
        DeleteFile splitDelete = split.getDeletes().get(0);
        assertEquals(splitDelete.getPath(), "/data/orders/deletes/d1.parquet");
        assertEquals(splitDelete.getRecordCount(), 25L);
        assertEquals(splitDelete.getPartialMax(), OptionalLong.of(25));
    }

    @Test
    public void testEncryptedDataFileFails()
    {
        DuckLakeDataFile dataFile = dataFile(
                "/data/orders/f2.parquet",
                "parquet",
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.of("secret-key"),
                OptionalLong.empty(),
                Optional.empty(),
                ImmutableMap.of());

        PrestoException exception = expectUnsupportedFeature(catalog(ImmutableList.of(dataFile), ImmutableList.of()));
        assertTrue(exception.getMessage().contains("/data/orders/f2.parquet"));
    }

    @Test
    public void testMappingIdDataFileFails()
    {
        DuckLakeDataFile dataFile = dataFile(
                "/data/orders/f3.parquet",
                "parquet",
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalLong.of(9),
                Optional.empty(),
                ImmutableMap.of());

        PrestoException exception = expectUnsupportedFeature(catalog(ImmutableList.of(dataFile), ImmutableList.of()));
        assertTrue(exception.getMessage().contains("/data/orders/f3.parquet"));
    }

    @Test
    public void testNonParquetDeleteFileFails()
    {
        DuckLakeDeleteFile deleteFile = deleteFile("/data/orders/deletes/d2.orc", "orc", 5L, 10L, OptionalLong.empty());
        DuckLakeDataFile dataFile = dataFile(
                "/data/orders/f4.parquet",
                "parquet",
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.of(deleteFile),
                ImmutableMap.of());

        PrestoException exception = expectUnsupportedFeature(catalog(ImmutableList.of(dataFile), ImmutableList.of()));
        assertTrue(exception.getMessage().contains("/data/orders/deletes/d2.orc"));
    }

    @Test
    public void testInlinedTablesFollowParquetSplits()
    {
        DuckLakeDataFile dataFile = dataFile(
                "/data/orders/f5.parquet",
                "parquet",
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                ImmutableMap.of());
        DuckLakeInlinedTable first = new DuckLakeInlinedTable("ducklake_inlined_data_1_1", 1L);
        DuckLakeInlinedTable second = new DuckLakeInlinedTable("ducklake_inlined_data_1_2", 2L);

        List<ConnectorSplit> splits = getSplits(catalog(ImmutableList.of(dataFile), ImmutableList.of(first, second)));

        assertEquals(splits.size(), 3);
        assertEquals(((DuckLakeSplit) splits.get(0)).getKind(), DuckLakeSplitKind.PARQUET);
        assertEquals(((DuckLakeSplit) splits.get(1)).getKind(), DuckLakeSplitKind.INLINED);
        assertEquals(((DuckLakeSplit) splits.get(1)).getInlinedTableName(), Optional.of("ducklake_inlined_data_1_1"));
        assertEquals(((DuckLakeSplit) splits.get(2)).getKind(), DuckLakeSplitKind.INLINED);
        assertEquals(((DuckLakeSplit) splits.get(2)).getInlinedTableName(), Optional.of("ducklake_inlined_data_1_2"));
    }

    @Test
    public void testSplitWeightScalesWithFileSize()
    {
        double minimumAssignedSplitWeight = new DuckLakeConfig().getMinimumAssignedSplitWeight();

        DuckLakeDataFile smallFile = dataFile(
                "/data/orders/small.parquet", "parquet", 1024L, OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), ImmutableMap.of());
        DuckLakeDataFile bigFile = dataFile(
                "/data/orders/big.parquet", "parquet", 1024L * 1024 * 1024, OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), ImmutableMap.of());

        List<ConnectorSplit> smallSplits = getSplits(catalog(ImmutableList.of(smallFile), ImmutableList.of()));
        List<ConnectorSplit> bigSplits = getSplits(catalog(ImmutableList.of(bigFile), ImmutableList.of()));

        assertEquals(((DuckLakeSplit) smallSplits.get(0)).getSplitWeight(), SplitWeight.fromProportion(minimumAssignedSplitWeight));
        assertEquals(((DuckLakeSplit) bigSplits.get(0)).getSplitWeight(), SplitWeight.fromProportion(1.0));
    }

    @Test
    public void testSplitSourceBatches()
    {
        DuckLakeDataFile dataFile = dataFile(
                "/data/orders/f6.parquet", "parquet", 1024L, OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), ImmutableMap.of());
        DuckLakeInlinedTable first = new DuckLakeInlinedTable("ducklake_inlined_data_1_1", 1L);
        DuckLakeInlinedTable second = new DuckLakeInlinedTable("ducklake_inlined_data_1_2", 2L);

        DuckLakeSplitManager splitManager = new DuckLakeSplitManager(catalog(ImmutableList.of(dataFile), ImmutableList.of(first, second)));
        ConnectorSplitSource source = splitManager.getSplits(null, session(), layoutHandle(), null);

        ConnectorSplitSource.ConnectorSplitBatch batch1 = source.getNextBatch(NOT_PARTITIONED, 2).join();
        assertEquals(batch1.getSplits().size(), 2);
        assertFalse(batch1.isNoMoreSplits());
        assertFalse(source.isFinished());

        ConnectorSplitSource.ConnectorSplitBatch batch2 = source.getNextBatch(NOT_PARTITIONED, 2).join();
        assertEquals(batch2.getSplits().size(), 1);
        assertTrue(batch2.isNoMoreSplits());
        assertTrue(source.isFinished());
    }

    @Test
    public void testPartitionPruningDropsNonMatchingFiles()
    {
        DuckLakeDataFile matching = dataFile(
                "/data/orders/f7.parquet", "parquet", 1024L, OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), ImmutableMap.of(0, Optional.of("7")));
        DuckLakeDataFile nonMatching = dataFile(
                "/data/orders/f8.parquet", "parquet", 1024L, OptionalLong.empty(), OptionalLong.empty(), Optional.empty(), OptionalLong.empty(), Optional.empty(), ImmutableMap.of(0, Optional.of("8")));

        DuckLakeSplitManager splitManager = new DuckLakeSplitManager(catalog(ImmutableList.of(matching, nonMatching), ImmutableList.of()));
        ConnectorSplitSource source = splitManager.getSplits(null, session(), layoutHandleWithIdentityPredicate(), null);
        List<ConnectorSplit> splits = source.getNextBatch(NOT_PARTITIONED, 100).join().getSplits();

        assertEquals(splits.size(), 1);
        assertEquals(((DuckLakeSplit) splits.get(0)).getPath(), "/data/orders/f7.parquet");
    }

    @Test
    public void testStatisticsPruningDropsNonMatchingFiles()
    {
        DuckLakeColumnHandle orderkeyColumn = DuckLakeColumnHandle.primitiveColumnHandle(2L, "orderkey", "int64", BigintType.BIGINT);
        DuckLakeFileColumnStats matchingStats = new DuckLakeFileColumnStats(
                2L, OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), Optional.of("1"), Optional.of("5"), Optional.empty());
        DuckLakeFileColumnStats nonMatchingStats = new DuckLakeFileColumnStats(
                2L, OptionalLong.empty(), OptionalLong.empty(), OptionalLong.empty(), Optional.of("100"), Optional.of("200"), Optional.empty());
        DuckLakeDataFile matching = dataFileWithStats("/data/orders/f9.parquet", ImmutableMap.of(2L, matchingStats));
        DuckLakeDataFile nonMatching = dataFileWithStats("/data/orders/f10.parquet", ImmutableMap.of(2L, nonMatchingStats));

        Map<String, DuckLakeColumnHandle> predicateColumns = ImmutableMap.of("orderkey", orderkeyColumn);
        TupleDomain<ColumnHandle> domainPredicate = TupleDomain.withColumnDomains(
                ImmutableMap.of(orderkeyColumn, Domain.create(ValueSet.ofRanges(Range.lessThan(BigintType.BIGINT, 10L)), false)));
        DuckLakeTableHandle table = new DuckLakeTableHandle(
                "test_schema",
                DuckLakeTableName.from("orders").withSnapshotId(5, false),
                TABLE.getTableId(),
                TABLE.getSchemaId(),
                TABLE.getPath(),
                schema());
        DuckLakeTableLayoutHandle layoutHandle = new DuckLakeTableLayoutHandle(table, domainPredicate, predicateColumns, Optional.empty());

        DuckLakeSplitManager splitManager = new DuckLakeSplitManager(catalog(ImmutableList.of(matching, nonMatching), ImmutableList.of()));
        ConnectorSplitSource source = splitManager.getSplits(null, session(), layoutHandle, null);
        List<ConnectorSplit> splits = source.getNextBatch(NOT_PARTITIONED, 100).join().getSplits();

        assertEquals(splits.size(), 1);
        assertEquals(((DuckLakeSplit) splits.get(0)).getPath(), "/data/orders/f9.parquet");
    }

    private static DuckLakeTableLayoutHandle layoutHandleWithIdentityPredicate()
    {
        DuckLakeTableHandle table = new DuckLakeTableHandle(
                "test_schema",
                DuckLakeTableName.from("orders").withSnapshotId(5, false),
                TABLE.getTableId(),
                TABLE.getSchemaId(),
                TABLE.getPath(),
                schemaWithIdentityPartitionField());
        DuckLakeColumnHandle idColumn = DuckLakeColumnHandle.primitiveColumnHandle(1L, "id", "int32", IntegerType.INTEGER);
        Map<String, DuckLakeColumnHandle> predicateColumns = ImmutableMap.of("id", idColumn);
        TupleDomain<ColumnHandle> domainPredicate = TupleDomain.withColumnDomains(ImmutableMap.of(idColumn, Domain.singleValue(IntegerType.INTEGER, 7L)));
        return new DuckLakeTableLayoutHandle(table, domainPredicate, predicateColumns, Optional.empty());
    }

    private static PrestoDuckLakeSchema schemaWithIdentityPartitionField()
    {
        DuckLakeColumnIdentity id = new DuckLakeColumnIdentity(1, "id", PRIMITIVE, "int32", ImmutableList.of());
        DuckLakePartitionField partitionField = new DuckLakePartitionField(0, 1L, "identity");
        return new PrestoDuckLakeSchema(ImmutableList.of(id), ImmutableMap.of(), ImmutableList.of(partitionField));
    }

    private static List<ConnectorSplit> getSplits(DuckLakeCatalog catalog)
    {
        DuckLakeSplitManager splitManager = new DuckLakeSplitManager(catalog);
        ConnectorSplitSource source = splitManager.getSplits(null, session(), layoutHandle(), null);
        return source.getNextBatch(NOT_PARTITIONED, 100).join().getSplits();
    }

    private static PrestoException expectUnsupportedFeature(DuckLakeCatalog catalog)
    {
        try {
            getSplits(catalog);
            fail("expected PrestoException");
            throw new AssertionError();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), DUCKLAKE_UNSUPPORTED_FEATURE.toErrorCode());
            return e;
        }
    }

    private static TestingConnectorSession session()
    {
        return new TestingConnectorSession(new DuckLakeSessionProperties(new DuckLakeConfig(), new CacheConfig()).getSessionProperties());
    }

    private static DuckLakeTableLayoutHandle layoutHandle()
    {
        DuckLakeTableHandle table = new DuckLakeTableHandle(
                "test_schema",
                DuckLakeTableName.from("orders").withSnapshotId(5, false),
                TABLE.getTableId(),
                TABLE.getSchemaId(),
                TABLE.getPath(),
                schema());
        Map<String, DuckLakeColumnHandle> predicateColumns = ImmutableMap.of();
        Optional<Set<DuckLakeColumnHandle>> requestedColumns = Optional.empty();
        return new DuckLakeTableLayoutHandle(table, TupleDomain.all(), predicateColumns, requestedColumns);
    }

    private static PrestoDuckLakeSchema schema()
    {
        DuckLakeColumnIdentity id = new DuckLakeColumnIdentity(1, "id", PRIMITIVE, "int32", ImmutableList.of());
        return new PrestoDuckLakeSchema(ImmutableList.of(id), ImmutableMap.of(), ImmutableList.of());
    }

    private static DuckLakeDataFile dataFile(
            String path,
            String fileFormat,
            long fileSizeBytes,
            OptionalLong rowIdStart,
            OptionalLong partialMax,
            Optional<String> encryptionKey,
            OptionalLong mappingId,
            Optional<DuckLakeDeleteFile> deleteFile,
            Map<Integer, Optional<String>> partitionValues)
    {
        return new DuckLakeDataFile(
                1L,
                path,
                fileFormat,
                100L,
                fileSizeBytes,
                OptionalLong.empty(),
                rowIdStart,
                OptionalLong.empty(),
                encryptionKey,
                mappingId,
                partialMax,
                deleteFile,
                partitionValues,
                ImmutableMap.of());
    }

    private static DuckLakeDataFile dataFileWithStats(String path, Map<Long, DuckLakeFileColumnStats> columnStats)
    {
        return new DuckLakeDataFile(
                1L,
                path,
                "parquet",
                100L,
                1024L,
                OptionalLong.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                OptionalLong.empty(),
                OptionalLong.empty(),
                Optional.empty(),
                ImmutableMap.of(),
                columnStats);
    }

    private static DuckLakeDeleteFile deleteFile(String path, String format, long deleteCount, long fileSizeBytes, OptionalLong partialMax)
    {
        return new DuckLakeDeleteFile(1L, path, format, deleteCount, fileSizeBytes, OptionalLong.empty(), Optional.empty(), partialMax);
    }

    private static DuckLakeCatalog catalog(List<DuckLakeDataFile> dataFiles, List<DuckLakeInlinedTable> inlinedTables)
    {
        return new FakeDuckLakeCatalog(dataFiles, inlinedTables);
    }

    private static class FakeDuckLakeCatalog
            implements DuckLakeCatalog
    {
        private final List<DuckLakeDataFile> dataFiles;
        private final List<DuckLakeInlinedTable> inlinedTables;

        FakeDuckLakeCatalog(List<DuckLakeDataFile> dataFiles, List<DuckLakeInlinedTable> inlinedTables)
        {
            this.dataFiles = dataFiles;
            this.inlinedTables = inlinedTables;
        }

        @Override
        public long getLatestSnapshotId()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<DuckLakeSnapshot> getSnapshot(long snapshotId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<DuckLakeSnapshot> getSnapshotAtOrBefore(Instant time)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeSnapshot> listSnapshots()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeSchema> listSchemas(long snapshotId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<DuckLakeSchema> getSchema(long snapshotId, String schemaName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeTable> listTables(long snapshotId, DuckLakeSchema schema)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Optional<DuckLakeTable> getTable(long snapshotId, DuckLakeSchema schema, String tableName)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeColumnRow> listColumns(long snapshotId, long tableId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakePartitionField> listPartitionFields(long snapshotId, long tableId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeDataFile> listDataFiles(long snapshotId, DuckLakeTable table)
        {
            return dataFiles;
        }

        @Override
        public Optional<DuckLakeTableStats> getTableStats(long tableId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeTableColumnStats> listTableColumnStats(long tableId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<DuckLakeInlinedTable> listInlinedTables(long tableId)
        {
            return inlinedTables;
        }

        @Override
        public DuckLakeInlinedRowSource openInlinedRows(long snapshotId, DuckLakeInlinedTable inlinedTable, List<String> columnNames)
        {
            throw new UnsupportedOperationException();
        }
    }
}
