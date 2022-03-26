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
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.TestingTypeManager;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordPageSource;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SplitWeight;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
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

import static com.facebook.presto.hive.CacheQuotaRequirement.NO_CACHE_REQUIREMENT;
import static com.facebook.presto.hive.CacheQuotaScope.PARTITION;
import static com.facebook.presto.hive.HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY;
import static com.facebook.presto.hive.TestHivePageSink.getColumnHandles;
import static com.facebook.presto.hive.util.HudiRealtimeSplitConverter.HUDI_BASEPATH_KEY;
import static com.facebook.presto.hive.util.HudiRealtimeSplitConverter.HUDI_DELTA_FILEPATHS_KEY;
import static com.facebook.presto.hive.util.HudiRealtimeSplitConverter.HUDI_MAX_COMMIT_TIME_KEY;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.NO_PREFERENCE;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestHivePageSourceProvider
{
    private static final String SCHEMA_NAME = "schema";
    private static final String TABLE_NAME = "table";
    private static final String PARTITION_NAME = "partition";

    @Test
    public void testGenerateCacheQuota()
    {
        HiveClientConfig config = new HiveClientConfig();

        HiveSplit split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,
                "file://test",
                0,
                10,
                10,
                Instant.now().toEpochMilli(),
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
                Optional.empty(),
                NO_CACHE_REQUIREMENT,
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                SplitWeight.standard());

        CacheQuota cacheQuota = HivePageSourceProvider.generateCacheQuota(split);
        CacheQuota expectedCacheQuota = new CacheQuota(".", Optional.empty());
        assertEquals(cacheQuota, expectedCacheQuota);

        split = new HiveSplit(
                SCHEMA_NAME,
                TABLE_NAME,
                PARTITION_NAME,
                "file://test",
                0,
                10,
                10,
                Instant.now().toEpochMilli(),
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
                Optional.empty(),
                new CacheQuotaRequirement(PARTITION, Optional.of(DataSize.succinctDataSize(1, DataSize.Unit.MEGABYTE))),
                Optional.empty(),
                ImmutableMap.of(),
                ImmutableSet.of(),
                SplitWeight.standard());

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

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(recordCursorProvider),
                ImmutableSet.of(hiveBatchPageSourceFactory),
                new Configuration(),
                new TestingConnectorSession(new HiveSessionProperties(
                        new HiveClientConfig().setUseRecordPageSourceForCustomSplit(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties()),
                new Path("/test/"),
                OptionalInt.empty(),
                0,
                100,
                200,
                Instant.now().toEpochMilli(),
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
                customSplitInfo);
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

        Optional<ConnectorPageSource> pageSource = HivePageSourceProvider.createHivePageSource(
                ImmutableSet.of(recordCursorProvider),
                ImmutableSet.of(hiveBatchPageSourceFactory),
                new Configuration(),
                new TestingConnectorSession(new HiveSessionProperties(
                        new HiveClientConfig().setUseRecordPageSourceForCustomSplit(true),
                        new OrcFileWriterConfig(),
                        new ParquetFileWriterConfig(),
                        new CacheConfig()).getSessionProperties()),
                new Path("/test/"),
                OptionalInt.empty(),
                0,
                100,
                200,
                Instant.now().toEpochMilli(),
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
                ImmutableMap.of());
        assertTrue(pageSource.isPresent());
        assertTrue(pageSource.get() instanceof HivePageSource);
    }

    static class MockHiveBatchPageSourceFactory
            implements HiveBatchPageSourceFactory
    {
        @Override
        public Optional<? extends ConnectorPageSource> createPageSource(Configuration configuration,
                ConnectorSession session,
                Path path,
                long start,
                long length,
                long fileSize,
                Storage storage,
                SchemaTableName tableName,
                Map<String, String> tableParameters,
                List<HiveColumnHandle> columns,
                TupleDomain<HiveColumnHandle> effectivePredicate,
                DateTimeZone hiveStorageTimeZone,
                HiveFileContext hiveFileContext,
                Optional<EncryptionInformation> encryptionInformation)
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
                Path path,
                long start,
                long length,
                long fileSize,
                Properties schema,
                List<HiveColumnHandle> columns,
                TupleDomain<HiveColumnHandle> effectivePredicate,
                DateTimeZone hiveStorageTimeZone,
                TypeManager typeManager,
                boolean s3SelectPushdownEnabled,
                Map<String, String> customSplitInfo)
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
}
