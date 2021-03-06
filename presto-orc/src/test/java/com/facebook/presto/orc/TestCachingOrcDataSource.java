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
package com.facebook.presto.orc;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.orc.OrcTester.Format;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.orc.OrcConf;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.AbstractOrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.facebook.presto.orc.AbstractOrcRecordReader.wrapWithCacheIfTinyStripes;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.writeOrcFileColumnHive;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

public class TestCachingOrcDataSource
{
    private static final int POSITION_COUNT = 50000;

    private TempFile tempFile;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        tempFile = new TempFile();
        Random random = new Random();
        List<String> values = Stream.generate(() -> Long.toHexString(random.nextLong())).limit(POSITION_COUNT).collect(toImmutableList());
        writeOrcFileColumnHive(
                tempFile.getFile(),
                ORC_12,
                createOrcRecordWriter(tempFile.getFile(), ORC_12, ZLIB, javaStringObjectInspector),
                VARCHAR,
                values);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        tempFile.close();
    }

    @Test
    public void testWrapWithCacheIfTinyStripes()
    {
        DataSize maxMergeDistance = new DataSize(1, Unit.MEGABYTE);
        DataSize tinyStripeThreshold = new DataSize(8, Unit.MEGABYTE);

        OrcAggregatedMemoryContext systemMemoryContext = new TestingHiveOrcAggregatedMemoryContext();

        OrcDataSource actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(),
                maxMergeDistance,
                tinyStripeThreshold,
                systemMemoryContext);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10, ImmutableList.of())),
                maxMergeDistance,
                tinyStripeThreshold,
                systemMemoryContext);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(
                        new StripeInformation(123, 3, 10, 10, 10, ImmutableList.of()),
                        new StripeInformation(123, 33, 10, 10, 10, ImmutableList.of()),
                        new StripeInformation(123, 63, 10, 10, 10, ImmutableList.of())),
                maxMergeDistance,
                tinyStripeThreshold,
                systemMemoryContext);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(
                        new StripeInformation(123, 3, 10, 10, 10, ImmutableList.of()),
                        new StripeInformation(123, 33, 10, 10, 10, ImmutableList.of()),
                        new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10, ImmutableList.of())),
                maxMergeDistance,
                tinyStripeThreshold,
                systemMemoryContext);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(
                        new StripeInformation(123, 3, 10, 10, 10, ImmutableList.of()),
                        new StripeInformation(123, 33, 10, 10, 10, ImmutableList.of()),
                        new StripeInformation(123, 63, 1048576 * 8 - 20 + 1, 10, 10, ImmutableList.of())),
                maxMergeDistance,
                tinyStripeThreshold,
                systemMemoryContext);
        assertNotInstanceOf(actual, CachingOrcDataSource.class);
    }

    @Test
    public void testTinyStripesReadCacheAt()
            throws IOException
    {
        DataSize maxMergeDistance = new DataSize(1, Unit.MEGABYTE);
        DataSize tinyStripeThreshold = new DataSize(8, Unit.MEGABYTE);

        OrcAggregatedMemoryContext systemMemoryContext = new TestingHiveOrcAggregatedMemoryContext();

        TestingOrcDataSource testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        CachingOrcDataSource cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(
                                new StripeInformation(123, 3, 10, 10, 10, ImmutableList.of()),
                                new StripeInformation(123, 33, 10, 10, 10, ImmutableList.of()),
                                new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10, ImmutableList.of())),
                        maxMergeDistance,
                        tinyStripeThreshold),
                systemMemoryContext.newOrcLocalMemoryContext(CachingOrcDataSource.class.getSimpleName()));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        // The allocated cache size is the length of the merged disk range 1048576 * 8.
        assertEquals(systemMemoryContext.getBytes(), 8 * 1048576);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(
                                new StripeInformation(123, 3, 10, 10, 10, ImmutableList.of()),
                                new StripeInformation(123, 33, 10, 10, 10, ImmutableList.of()),
                                new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10, ImmutableList.of())),
                        maxMergeDistance,
                        tinyStripeThreshold),
                systemMemoryContext.newOrcLocalMemoryContext(CachingOrcDataSource.class.getSimpleName()));
        cachingOrcDataSource.readCacheAt(62); // read at the end of a stripe
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        // The newly allocated cache size is the length of the merged disk range 1048576 * 8 so the total size is 8 * 1048576 * 2.
        assertEquals(systemMemoryContext.getBytes(), 8 * 1048576 * 2);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(
                                new StripeInformation(123, 3, 1, 1, 1, ImmutableList.of()),
                                new StripeInformation(123, 4, 1048576, 1048576, 1048576 * 3, ImmutableList.of()),
                                new StripeInformation(123, 4 + 1048576 * 5, 1048576, 1048576, 1048576, ImmutableList.of())),
                        maxMergeDistance,
                        tinyStripeThreshold),
                systemMemoryContext.newOrcLocalMemoryContext(CachingOrcDataSource.class.getSimpleName()));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 1 + 1048576 * 5)));
        cachingOrcDataSource.readCacheAt(4 + 1048576 * 5);
        // The newly allocated cache size is the length of the first merged disk range 1048576 * 5 + 1.
        assertEquals(systemMemoryContext.getBytes(), 8 * 1048576 * 2 + 1048576 * 5 + 1);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(4 + 1048576 * 5, 3 * 1048576)));
    }

    @Test
    public void testIntegration()
            throws IOException
    {
        // tiny file
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), true));
        doIntegration(orcDataSource, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        assertEquals(orcDataSource.getReadCount(), 1); // read entire file at once

        // tiny stripes
        orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), true));
        doIntegration(orcDataSource, new DataSize(400, Unit.KILOBYTE), new DataSize(400, Unit.KILOBYTE), new DataSize(400, Unit.KILOBYTE));
        assertEquals(orcDataSource.getReadCount(), 3); // footer, first few stripes, last few stripes
    }

    public void doIntegration(TestingOrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize maxReadSize, DataSize tinyStripeThreshold)
            throws IOException
    {
        OrcAggregatedMemoryContext systemMemoryContext = new TestingHiveOrcAggregatedMemoryContext();

        OrcReader orcReader = new OrcReader(
                orcDataSource,
                ORC,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                new OrcReaderOptions(maxMergeDistance, tinyStripeThreshold, new DataSize(1, Unit.MEGABYTE), false),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY);
        // 1 for reading file footer
        assertEquals(orcDataSource.getReadCount(), 1);
        List<StripeInformation> stripes = orcReader.getFooter().getStripes();
        // Sanity check number of stripes. This can be three or higher because of orc writer low memory mode.
        assertGreaterThanOrEqual(stripes.size(), 3);
        //verify wrapped by CachingOrcReader
        assertInstanceOf(wrapWithCacheIfTinyStripes(orcDataSource, stripes, maxMergeDistance, tinyStripeThreshold, systemMemoryContext), CachingOrcDataSource.class);

        OrcBatchRecordReader orcRecordReader = orcReader.createBatchRecordReader(
                ImmutableMap.of(0, VARCHAR),
                (numberOfRows, statisticsByColumnIndex) -> true,
                HIVE_STORAGE_TIME_ZONE,
                new TestingHiveOrcAggregatedMemoryContext(),
                INITIAL_BATCH_SIZE);
        int positionCount = 0;
        while (true) {
            int batchSize = orcRecordReader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = orcRecordReader.readBlock(0);
            positionCount += block.getPositionCount();
        }
        assertEquals(positionCount, POSITION_COUNT);
    }

    public static <T, U extends T> void assertNotInstanceOf(T actual, Class<U> expectedType)
    {
        assertNotNull(actual, "actual is null");
        assertNotNull(expectedType, "expectedType is null");
        if (expectedType.isInstance(actual)) {
            fail(String.format("expected:<%s> to not be an instance of <%s>", actual, expectedType.getName()));
        }
    }

    private static FileSinkOperator.RecordWriter createOrcRecordWriter(File outputFile, Format format, CompressionKind compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        OrcConf.WRITE_FORMAT.setString(jobConf, format == ORC_12 ? "0.12" : "0.11");
        OrcConf.COMPRESS.setString(jobConf, compression.name());

        Properties tableProperties = new Properties();
        tableProperties.setProperty(IOConstants.COLUMNS, "test");
        tableProperties.setProperty(IOConstants.COLUMNS_TYPES, columnObjectInspector.getTypeName());
        tableProperties.setProperty(OrcConf.STRIPE_SIZE.getAttribute(), "120000");

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                tableProperties,
                () -> {});
    }

    private static class FakeOrcDataSource
            implements OrcDataSource
    {
        public static final FakeOrcDataSource INSTANCE = new FakeOrcDataSource();

        @Override
        public OrcDataSourceId getId()
        {
            return new OrcDataSourceId("fake");
        }

        @Override
        public long getReadBytes()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getReadTimeNanos()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getSize()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void readFully(long position, byte[] buffer)
        {
            // do nothing
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
        {
            // do nothing
        }

        @Override
        public <K> Map<K, OrcDataSourceInput> readFully(Map<K, DiskRange> diskRanges)
        {
            throw new UnsupportedOperationException();
        }
    }
}
