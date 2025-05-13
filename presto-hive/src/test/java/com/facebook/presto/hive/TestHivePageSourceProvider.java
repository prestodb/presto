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
package com.facebook.presto.hive;

import com.facebook.presto.cache.CacheConfig;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitContext;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.joda.time.DateTimeZone;
import org.testng.annotations.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;

import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.AGGREGATED;
import static com.facebook.presto.hive.BaseHiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.CacheQuotaRequirement.NO_CACHE_REQUIREMENT;
import static com.facebook.presto.hive.CacheQuotaScope.PARTITION;
import static com.facebook.presto.hive.HiveStorageFormat.ORC;
import static com.facebook.presto.hive.HiveStorageFormat.RCBINARY;
import static com.facebook.presto.hive.HiveTestUtils.HDFS_ENVIRONMENT;
import static com.facebook.presto.hive.HiveTestUtils.HIVE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.METADATA;
import static com.facebook.presto.hive.HiveTestUtils.METASTORE_CLIENT_CONFIG;
import static com.facebook.presto.hive.HiveTestUtils.ROW_EXPRESSION_SERVICE;
import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.HiveTestUtils.getDefaultHiveRecordCursorProvider;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY;
import static com.facebook.presto.hive.TestHiveMetadataUpdateHandle.TEST_TABLE_NAME;
import static com.facebook.presto.hive.TestHivePageSink.getColumnHandles;
import static com.facebook.presto.hive.metastore.thrift.MockHiveMetastoreClient.TEST_DATABASE;
import static com.facebook.presto.hive.util.HudiRealtimeSplitConverter.HUDI_BASEPATH_KEY;
import static com.facebook.presto.hive.util.HudiRealtimeSplitConverter.HUDI_DELTA_FILEPATHS_KEY;
import static com.facebook.presto.hive.util.HudiRealtimeSplitConverter.HUDI_MAX_COMMIT_TIME_KEY;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static org.testng.Assert.assertTrue;

public class TestHivePageSourceProvider
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String PARTITION_NAME = "partition";

    private static final ColumnHandle LONG_COLUMN = new HiveColumnHandle(
            "test_column",
            HIVE_LONG,
            HIVE_LONG.getTypeSignature(),
            5,
            REGULAR,
            Optional.empty(),
            ImmutableList.of(),
            Optional.empty());

    private static final ColumnHandle LONG_AGGREGATED_COLUMN = new HiveColumnHandle(
            "test_column",
            HIVE_LONG,
            HIVE_LONG.getTypeSignature(),
            5,
            AGGREGATED,
            Optional.empty(),
            ImmutableList.of(),
            Optional.of(AggregationNodeUtils.count(FunctionAndTypeManager.createTestFunctionAndTypeManager())));

    public HivePageSourceProvider createPageSourceProvider()
    {
        return new HivePageSourceProvider(
                HIVE_CLIENT_CONFIG,
                HDFS_ENVIRONMENT,
                getDefaultHiveRecordCursorProvider(HIVE_CLIENT_CONFIG, METASTORE_CLIENT_CONFIG),
                ImmutableSet.of(new MockOrcBatchPageSourceFactory(), new MockRcBinaryBatchPageSourceFactory()),
                ImmutableSet.of(new MockOrcSelectivePageSourceFactory()),
                ImmutableSet.of(new MockOrcAggregatedPageSourceFactory()),
                METADATA.getFunctionAndTypeManager(),
                ROW_EXPRESSION_SERVICE);
    }

    @Test
    public void testGenerateCacheQuota()
    {
        HiveClientConfig config = new HiveClientConfig();

        HiveFileSplit fileSplit = new HiveFileSplit("file://test",
                0,
                10,
                10,
                Instant.now().toEpochMilli(),
                Optional.empty(),
                ImmutableMap.of(),
                0);
        HiveSplit split = new HiveSplit(
                fileSplit,
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,

                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(), config.getHiveStorageFormat().getInputFormat(), config.getHiveStorageFormat().getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                NO_CACHE_REQUIREMENT,
                Optional.empty(),
                ImmutableSet.of(),
                SplitWeight.standard(),
                Optional.empty());

        CacheQuota cacheQuota = HivePageSourceProvider.generateCacheQuota(split);
        CacheQuota expectedCacheQuota = new CacheQuota(".", Optional.empty());
        assertEquals(cacheQuota, expectedCacheQuota);

        split = new HiveSplit(
                fileSplit,
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,
                new Storage(
                        StorageFormat.create(config.getHiveStorageFormat().getSerDe(), config.getHiveStorageFormat().getInputFormat(), config.getHiveStorageFormat().getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                new CacheQuotaRequirement(PARTITION, Optional.of(DataSize.succinctDataSize(1, DataSize.Unit.MEGABYTE))),
                Optional.empty(),
                ImmutableSet.of(),
                SplitWeight.standard(),
                Optional.empty());

        cacheQuota = HivePageSourceProvider.generateCacheQuota(split);
        expectedCacheQuota = new CacheQuota(SCHEMA_NAME + "." + TABLE_NAME + "." + PARTITION_NAME, Optional.of(DataSize.succinctDataSize(1, DataSize.Unit.MEGABYTE)));
        assertEquals(cacheQuota, expectedCacheQuota);
    }

    @Test
    public void testUseRecordReaderWithInputFormatAnnotationAndCustomSplit()
    {
        StorageFormat storageFormat = StorageFormat.create(ParquetHiveSerDe.class.getName(), HoodieParquetInputFormat.class.getName(), "");
        Storage storage = new Storage(storageFormat, "test", Optional.empty(), true, ImmutableMap.of(), ImmutableMap.of());
        Map<String, String> customSplitInfo = ImmutableMap.of(
                CUSTOM_FILE_SPLIT_CLASS_KEY, HoodieRealtimeFileSplit.class.getName(),
                HUDI_BASEPATH_KEY, "/test/file.parquet",
                HUDI_DELTA_FILEPATHS_KEY, "/test/.file_100.log",
                HUDI_MAX_COMMIT_TIME_KEY, "100");
        HiveRecordCursorProvider recordCursorProvider = new MockHiveRecordCursorProvider();
        HiveBatchPageSourceFactory hiveBatchPageSourceFactory = new MockHiveBatchPageSourceFactory();

        HiveFileSplit fileSplit = new HiveFileSplit(
                "/test/",
                0,
                100,
                200,
                Instant.now().toEpochMilli(),
                Optional.empty(),
                customSplitInfo,
                0);
        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(recordCursorProvider),
                ImmutableSet.of(hiveBatchPageSourceFactory),
                new Configuration(),
                new TestingConnectorSession(new HiveSessionProperties(
                        new HiveClientConfig().setUseRecordPageSourceForCustomSplit(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties()),
                fileSplit,
                OptionalInt.empty(),
                storage,
                TupleDomain.none(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableList.of(),
                DateTimeZone.UTC,
                new TestingTypeManager(),
                new SchemaTableName("test", "test"),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                0,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                null,
                null,
                false,
                null,
                Optional.empty(),
                Optional.empty());
        assertTrue(pageSource.isPresent());
        assertTrue(pageSource.get() instanceof RecordPageSource);
    }

    @Test
    public void testNotUseRecordReaderWithInputFormatAnnotationWithoutCustomSplit()
    {
        StorageFormat storageFormat = StorageFormat.create(ParquetHiveSerDe.class.getName(), HoodieParquetInputFormat.class.getName(), "");
        Storage storage = new Storage(storageFormat, "test", Optional.empty(), true, ImmutableMap.of(), ImmutableMap.of());
        HiveRecordCursorProvider recordCursorProvider = new MockHiveRecordCursorProvider();
        HiveBatchPageSourceFactory hiveBatchPageSourceFactory = new MockHiveBatchPageSourceFactory();

        HiveFileSplit fileSplit = new HiveFileSplit(
                "/test/",
                0,
                100,
                200,
                Instant.now().toEpochMilli(),
                Optional.empty(),
                ImmutableMap.of(),
                0);

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(recordCursorProvider),
                ImmutableSet.of(hiveBatchPageSourceFactory),
                new Configuration(),
                new TestingConnectorSession(new HiveSessionProperties(
                        new HiveClientConfig().setUseRecordPageSourceForCustomSplit(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties()),
                fileSplit,
                OptionalInt.empty(),
                storage,
                TupleDomain.none(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableList.of(),
                DateTimeZone.UTC,
                new TestingTypeManager(),
                new SchemaTableName("test", "test"),
                ImmutableList.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                0,
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                null,
                null,
                false,
                null,
                Optional.empty(),
                Optional.empty());
        assertTrue(pageSource.isPresent());
        assertTrue(pageSource.get() instanceof HivePageSource);
    }

    @Test
    public void testUsesPageSourceForPartition()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        RuntimeStats runtimeStats = new RuntimeStats();
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(ORC),
                getHiveTableLayout(false, false, false),
                ImmutableList.of(LONG_COLUMN),
                new SplitContext(false),
                runtimeStats);
        assertTrue(pageSource instanceof HivePageSource, format("pageSource was %s", pageSource.getClass().getSimpleName()));
        assertTrue(((HivePageSource) pageSource).getPageSource() instanceof MockOrcBatchPageSource,
                format("pageSoruce was %s", ((HivePageSource) pageSource).getPageSource().getClass().getSimpleName()));

        pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(RCBINARY),
                getHiveTableLayout(false, false, false),
                ImmutableList.of(LONG_COLUMN),
                new SplitContext(false),
                runtimeStats);
        assertTrue(pageSource instanceof HivePageSource, format("pageSource  was %s", pageSource.getClass().getSimpleName()));
        assertTrue(((HivePageSource) pageSource).getPageSource() instanceof MockRcBinaryBatchPageSource,
                format("pageSource  was %s", ((HivePageSource) pageSource).getPageSource().getClass().getSimpleName()));

        pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(ORC),
                getHiveTableLayout(true, false, false),
                ImmutableList.of(LONG_COLUMN),
                new SplitContext(false),
                runtimeStats);
        assertTrue(pageSource instanceof MockOrcSelectivePageSource, format("pageSource  was %s", pageSource.getClass().getSimpleName()));
    }

    @Test
    public void testWrapsInFilteringPageSourceWhenNoSelectivePageSource()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(RCBINARY),
                getHiveTableLayout(true, false, false),
                ImmutableList.of(),
                new SplitContext(false),
                new RuntimeStats());
        assertTrue(pageSource instanceof FilteringPageSource, format("pageSource was %s", pageSource.getClass().getSimpleName()));
    }

    @Test
    public void testAggregatedPageSource()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(ORC),
                getHiveTableLayout(true, true, false),
                ImmutableList.of(LONG_AGGREGATED_COLUMN),
                new SplitContext(false),
                new RuntimeStats());
        assertTrue(pageSource instanceof MockOrcAggregatedPageSource, format("pageSource %s", pageSource.getClass().getSimpleName()));
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "Table testdb.table has file of format org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe that does not support partial aggregation pushdown. " +
                    "Set session property \\[catalog\\-name\\].pushdown_partial_aggregations_into_scan=false and execute query again.")
    public void testFailsWhenNoAggregatedPageSourceAvailable()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        ConnectorPageSource pageSource = pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(RCBINARY),
                getHiveTableLayout(false, true, false),
                ImmutableList.of(LONG_AGGREGATED_COLUMN),
                new SplitContext(false),
                new RuntimeStats());
    }

    @Test(expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "Partial aggregation pushdown is not supported when footer stats are unreliable. " +
                    "Table testdb.table has file file://test with unreliable footer stats. " +
                    "Set session property \\[catalog\\-name\\].pushdown_partial_aggregations_into_scan=false and execute query again.")
    public void testFailsWhenFooterStatsUnreliable()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(ORC),
                getHiveTableLayout(false, true, true),
                ImmutableList.of(LONG_AGGREGATED_COLUMN),
                new SplitContext(false),
                new RuntimeStats());
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Not all columns are of 'AGGREGATED' type")
    public void testFailsWhenMixOfAggregatedAndRegularColumns()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                getHiveSplit(ORC),
                getHiveTableLayout(false, true, false),
                ImmutableList.of(LONG_COLUMN, LONG_AGGREGATED_COLUMN),
                new SplitContext(false),
                new RuntimeStats());
    }

    @Test
    public void testCreatePageSource_withRowID()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        HiveSplit hiveSplit = makeHiveSplit(ORC, Optional.of(new byte[20]));
        try (HivePageSource pageSource = (HivePageSource) pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                hiveSplit,
                getHiveTableLayout(false, true, false),
                ImmutableList.of(LONG_COLUMN, HiveColumnHandle.rowIdColumnHandle()),
                new SplitContext(false),
                new RuntimeStats())) {
            assertEquals(0, pageSource.getCompletedBytes());
        }
    }

    public void testCreatePageSource_withRowIDMissingPartitionComponent()
    {
        HivePageSourceProvider pageSourceProvider = createPageSourceProvider();
        HiveSplit hiveSplit = getHiveSplit(ORC);
        pageSourceProvider.createPageSource(
                new HiveTransactionHandle(),
                SESSION,
                hiveSplit,
                getHiveTableLayout(false, true, false),
                ImmutableList.of(LONG_COLUMN, HiveColumnHandle.rowIdColumnHandle()),
                new SplitContext(false),
                new RuntimeStats());
    }

    private static ConnectorTableLayoutHandle getHiveTableLayout(boolean pushdownFilterEnabled, boolean partialAggregationsPushedDown, boolean footerStatsUnreliable)
    {
        return new HiveTableLayoutHandle(
                new SchemaTableName(TEST_DATABASE, TEST_TABLE_NAME),
                TEST_TABLE_NAME,
                ImmutableList.of(),
                ImmutableList.of(), // TODO fill out columns
                ImmutableMap.of(),
                TupleDomain.all(), // none
                TRUE_CONSTANT,
                ImmutableMap.of(),
                TupleDomain.all(),
                Optional.empty(),
                Optional.empty(),
                pushdownFilterEnabled,
                "layout",
                Optional.empty(),
                partialAggregationsPushedDown,
                true,
                footerStatsUnreliable);
    }

    private static HiveSplit getHiveSplit(HiveStorageFormat hiveStorageFormat)
    {
        return makeHiveSplit(hiveStorageFormat, Optional.empty());
    }

    private static HiveSplit makeHiveSplit(HiveStorageFormat hiveStorageFormat, Optional<byte[]> rowIDPartitionComponent)
    {
        HiveFileSplit fileSplit = new HiveFileSplit("file://test",
                0,
                10,
                10,
                Instant.now().toEpochMilli(),
                Optional.empty(),
                ImmutableMap.of(),
                0);

        return new HiveSplit(
                fileSplit,
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,

                new Storage(
                        StorageFormat.create(hiveStorageFormat.getSerDe(), hiveStorageFormat.getInputFormat(), hiveStorageFormat.getOutputFormat()),
                        "location",
                        Optional.empty(),
                        false,
                        ImmutableMap.of(),
                        ImmutableMap.of()),
                ImmutableList.of(),
                ImmutableList.of(),
                OptionalInt.empty(),
                OptionalInt.empty(),
                NO_PREFERENCE,
                getColumnHandles().size(),
                TableToPartitionMapping.empty(),
                Optional.empty(),
                false,
                NO_CACHE_REQUIREMENT,
                Optional.empty(),
                ImmutableSet.of(),
                SplitWeight.standard(),
                rowIDPartitionComponent);
    }

    static class MockHiveBatchPageSourceFactory
            implements HiveBatchPageSourceFactory
    {
        @Override
        public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration,
                ConnectorSession session,
                HiveFileSplit fileSplit,
                Storage storage,
                SchemaTableName tableName,
                Map<String, String> tableParameters,
                List<HiveColumnHandle> columns,
                TupleDomain<HiveColumnHandle> effectivePredicate,
                DateTimeZone hiveStorageTimeZone,
                HiveFileContext hiveFileContext,
                Optional<EncryptionInformation> encryptionInformation,
                Optional<byte[]> rowIdPartitionComponent)
        {
            return Optional.of(new MockPageSource());
        }
    }

    static class MockPageSource
            implements ConnectorPageSource
    {
        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getCompletedPositions()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public boolean isFinished()
        {
            return false;
        }

        @Override
        public Page getNextPage()
        {
            return null;
        }

        @Override
        public long getSystemMemoryUsage()
        {
            return 0;
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

    static class MockHiveRecordCursorProvider
            implements HiveRecordCursorProvider
    {
        @Override
        public Optional<RecordCursor> createRecordCursor(Configuration configuration,
                ConnectorSession session,
                HiveFileSplit fileSplit,
                Properties schema,
                List<HiveColumnHandle> columns,
                TupleDomain<HiveColumnHandle> effectivePredicate,
                DateTimeZone hiveStorageTimeZone,
                TypeManager typeManager,
                boolean s3SelectPushdownEnabled)
        {
            return Optional.of(new MockRecordCursor());
        }
    }

    static class MockRecordCursor
            implements RecordCursor
    {
        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return null;
        }

        @Override
        public boolean advanceNextPosition()
        {
            return false;
        }

        @Override
        public boolean getBoolean(int field)
        {
            return false;
        }

        @Override
        public long getLong(int field)
        {
            return 0;
        }

        @Override
        public double getDouble(int field)
        {
            return 0;
        }

        @Override
        public Slice getSlice(int field)
        {
            return null;
        }

        @Override
        public Object getObject(int field)
        {
            return null;
        }

        @Override
        public boolean isNull(int field)
        {
            return false;
        }

        @Override
        public void close()
        {
        }
    }

    private static class MockOrcBatchPageSourceFactory
            implements HiveBatchPageSourceFactory
    {
        @Override
        public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration,
                ConnectorSession session,
                HiveFileSplit fileSplit,
                Storage storage,
                SchemaTableName tableName,
                Map<String, String> tableParameters,
                List<HiveColumnHandle> columns,
                TupleDomain<HiveColumnHandle> effectivePredicate,
                DateTimeZone hiveStorageTimeZone,
                HiveFileContext hiveFileContext,
                Optional<EncryptionInformation> encryptionInformation,
                Optional<byte[]> rowIdPartitionComponent)
        {
            if (!OrcSerde.class.getName().equals(storage.getStorageFormat().getSerDe())) {
                return Optional.empty();
            }
            return Optional.of(new MockOrcBatchPageSource());
        }
    }

    private static class MockOrcSelectivePageSourceFactory
            implements HiveSelectivePageSourceFactory
    {
        @Override
        public Optional<? extends ConnectorPageSource> createPageSource(
                Configuration configuration,
                ConnectorSession session,
                HiveFileSplit fileSplit,
                Storage storage,
                List<HiveColumnHandle> columns,
                Map<Integer,
                String> prefilledValues,
                Map<Integer,
                HiveCoercer> coercers,
                Optional<BucketAdaptation> bucketAdaptation,
                List<Integer> outputColumns,
                TupleDomain<Subfield> domainPredicate,
                RowExpression remainingPredicate,
                DateTimeZone hiveStorageTimeZone,
                HiveFileContext hiveFileContext,
                Optional<EncryptionInformation> encryptionInformation,
                boolean appendRowNumberEnabled,
                Optional<byte[]> rowIDPartitionComponent)
        {
            if (!OrcSerde.class.getName().equals(storage.getStorageFormat().getSerDe())) {
                return Optional.empty();
            }
            return Optional.of(new MockOrcSelectivePageSource());
        }
    }

    private static class MockOrcAggregatedPageSourceFactory
            implements HiveAggregatedPageSourceFactory
    {
        @Override
        public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration, ConnectorSession session, HiveFileSplit fileSplit, Storage storage, List<HiveColumnHandle> columns, HiveFileContext hiveFileContext, Optional<EncryptionInformation> encryptionInformation)
        {
            if (!OrcSerde.class.getName().equals(storage.getStorageFormat().getSerDe())) {
                return Optional.empty();
            }
            return Optional.of(new MockOrcAggregatedPageSource());
        }
    }

    private static class MockRcBinaryBatchPageSourceFactory
            implements HiveBatchPageSourceFactory
    {
        public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration,
                ConnectorSession session,
                HiveFileSplit fileSplit,
                Storage storage,
                SchemaTableName tableName,
                Map<String, String> tableParameters,
                List<HiveColumnHandle> columns,
                TupleDomain<HiveColumnHandle> effectivePredicate,
                DateTimeZone hiveStorageTimeZone,
                HiveFileContext hiveFileContext,
                Optional<EncryptionInformation> encryptionInformation,
                Optional<byte[]> rowIdPartitionComponent)
        {
            if (!storage.getStorageFormat().getSerDe().equals(LazyBinaryColumnarSerDe.class.getName())) {
                return Optional.empty();
            }
            return Optional.of(new MockRcBinaryBatchPageSource());
        }
    }

    private static class MockOrcBatchPageSource
            extends MockPageSource {}

    private static class MockOrcSelectivePageSource
            extends MockPageSource {}

    private static class MockOrcAggregatedPageSource
            extends MockPageSource {}

    private static class MockRcBinaryBatchPageSource
            extends MockPageSource {}
}
