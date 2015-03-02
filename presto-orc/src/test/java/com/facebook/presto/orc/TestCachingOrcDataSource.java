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

import com.facebook.presto.orc.OrcTester.Compression;
import com.facebook.presto.orc.OrcTester.Format;
import com.facebook.presto.orc.OrcTester.TempFile;
import com.facebook.presto.orc.memory.AggregatedMemoryContext;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.spi.block.Block;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.FixedLengthSliceInput;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Stream;

import static com.facebook.presto.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.facebook.presto.orc.OrcRecordReader.wrapWithCacheIfTinyStripes;
import static com.facebook.presto.orc.OrcTester.Compression.NONE;
import static com.facebook.presto.orc.OrcTester.Compression.ZLIB;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.writeOrcColumn;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static io.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static io.airlift.testing.Assertions.assertInstanceOf;
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
        tempFile = new TempFile("presto_test_cods", "orc");
        Random random = new Random();
        Iterator<String> iterator = Stream.generate(() -> Long.toHexString(random.nextLong())).limit(POSITION_COUNT).iterator();
        writeOrcColumn(tempFile.getFile(), ORC_12, createOrcRecordWriter(tempFile.getFile(), ORC_12, ZLIB, javaStringObjectInspector), javaStringObjectInspector, iterator);
    }

    @AfterClass
    public void tearDown()
            throws Exception
    {
        tempFile.close();
    }

    @Test
    public void testWrapWithCacheIfTinyStripes()
            throws IOException
    {
        DataSize maxMergeDistance = new DataSize(1, Unit.MEGABYTE);
        DataSize maxReadSize = new DataSize(8, Unit.MEGABYTE);

        OrcDataSource actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(),
                maxMergeDistance,
                maxReadSize);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 10, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        assertInstanceOf(actual, CachingOrcDataSource.class);

        actual = wrapWithCacheIfTinyStripes(
                FakeOrcDataSource.INSTANCE,
                ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20 + 1, 10, 10)),
                maxMergeDistance,
                maxReadSize);
        assertNotInstanceOf(actual, CachingOrcDataSource.class);
    }

    @Test
    public void testTinyStripesReadCacheAt()
            throws IOException
    {
        DataSize maxMergeDistance = new DataSize(1, Unit.MEGABYTE);
        DataSize maxReadSize = new DataSize(8, Unit.MEGABYTE);

        TestingOrcDataSource testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        CachingOrcDataSource cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        maxReadSize));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 10, 10, 10), new StripeInformation(123, 33, 10, 10, 10), new StripeInformation(123, 63, 1048576 * 8 - 20, 10, 10)),
                        maxMergeDistance,
                        maxReadSize));
        cachingOrcDataSource.readCacheAt(62); // read at the end of a stripe
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 60)));
        cachingOrcDataSource.readCacheAt(63);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(63, 8 * 1048576)));

        testingOrcDataSource = new TestingOrcDataSource(FakeOrcDataSource.INSTANCE);
        cachingOrcDataSource = new CachingOrcDataSource(
                testingOrcDataSource,
                createTinyStripesRangeFinder(
                        ImmutableList.of(new StripeInformation(123, 3, 1, 0, 0), new StripeInformation(123, 4, 1048576, 1048576, 1048576 * 3), new StripeInformation(123, 4 + 1048576 * 5, 1048576, 1048576, 1048576)),
                        maxMergeDistance,
                        maxReadSize));
        cachingOrcDataSource.readCacheAt(3);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(3, 1 + 1048576 * 5)));
        cachingOrcDataSource.readCacheAt(4 + 1048576 * 5);
        assertEquals(testingOrcDataSource.getLastReadRanges(), ImmutableList.of(new DiskRange(4 + 1048576 * 5, 3 * 1048576)));
    }

    @Test
    public void testIntegration()
            throws IOException
    {
        // tiny file
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE)));
        doIntegration(orcDataSource, new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE));
        assertEquals(orcDataSource.getReadCount(), 1); // read entire file at once

        // tiny stripes
        orcDataSource = new TestingOrcDataSource(
                new FileOrcDataSource(tempFile.getFile(), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE), new DataSize(1, Unit.MEGABYTE)));
        doIntegration(orcDataSource, new DataSize(400, Unit.KILOBYTE), new DataSize(400, Unit.KILOBYTE));
        assertEquals(orcDataSource.getReadCount(), 3); // footer, first few stripes, last few stripes
    }

    public void doIntegration(TestingOrcDataSource orcDataSource, DataSize maxMergeDistance, DataSize maxReadSize)
            throws IOException
    {
        OrcReader orcReader = new OrcReader(orcDataSource, new OrcMetadataReader(), maxMergeDistance, maxReadSize);
        // 1 for reading file footer
        assertEquals(orcDataSource.getReadCount(), 1);
        List<StripeInformation> stripes = orcReader.getFooter().getStripes();
        // Sanity check number of stripes. This can be three or higher because of orc writer low memory mode.
        assertGreaterThanOrEqual(stripes.size(), 3);
        //verify wrapped by CachingOrcReader
        assertInstanceOf(wrapWithCacheIfTinyStripes(orcDataSource, stripes, maxMergeDistance, maxReadSize), CachingOrcDataSource.class);

        OrcRecordReader orcRecordReader = orcReader.createRecordReader(
                ImmutableMap.of(0, VARCHAR),
                (numberOfRows, statisticsByColumnIndex) -> true,
                HIVE_STORAGE_TIME_ZONE,
                new AggregatedMemoryContext());
        int positionCount = 0;
        while (true) {
            int batchSize = orcRecordReader.nextBatch();
            if (batchSize <= 0) {
                break;
            }
            Block block = orcRecordReader.readBlock(VARCHAR, 0);
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

    private static FileSinkOperator.RecordWriter createOrcRecordWriter(File outputFile, Format format, Compression compression, ObjectInspector columnObjectInspector)
            throws IOException
    {
        JobConf jobConf = new JobConf();
        jobConf.set("hive.exec.orc.write.format", format == ORC_12 ? "0.12" : "0.11");
        jobConf.set("hive.exec.orc.default.compress", compression.name());
        ReaderWriterProfiler.setProfilerOptions(jobConf);

        Properties tableProperties = new Properties();
        tableProperties.setProperty("columns", "test");
        tableProperties.setProperty("columns.types", columnObjectInspector.getTypeName());
        tableProperties.setProperty("orc.stripe.size", "1200000");

        return new OrcOutputFormat().getHiveRecordWriter(
                jobConf,
                new Path(outputFile.toURI()),
                Text.class,
                compression != NONE,
                tableProperties,
                () -> { });
    }

    private static class FakeOrcDataSource
            implements OrcDataSource
    {
        public static final FakeOrcDataSource INSTANCE = new FakeOrcDataSource();

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
                throws IOException
        {
            // do nothing
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            // do nothing
        }

        @Override
        public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
                throws IOException
        {
            throw new UnsupportedOperationException();
        }
    }
}
