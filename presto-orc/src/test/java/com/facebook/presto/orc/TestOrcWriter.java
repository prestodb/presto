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

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.statistics.hll.HyperLogLog;
import com.facebook.presto.orc.writer.StreamLayoutFactory;
import com.facebook.presto.orc.writer.StreamLayoutFactory.ColumnSizeLayoutFactory;
import com.facebook.presto.orc.writer.StreamLayoutFactory.StreamSizeLayoutFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.StripeReader.isIndexStream;
import static com.facebook.presto.orc.TestingOrcPredicate.ORC_ROW_GROUP_SIZE;
import static com.facebook.presto.orc.TestingOrcPredicate.ORC_STRIPE_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.facebook.presto.orc.metadata.statistics.hll.HyperLogLogSerde.HLL_USER_METADATA_KEY;
import static com.facebook.presto.orc.metadata.statistics.hll.HyperLogLogSerde.deserialize;
import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static java.lang.Float.floatToRawIntBits;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcWriter
{
    @DataProvider(name = "compressionLevels")
    public static Object[][] zstdCompressionLevels()
    {
        ImmutableList.Builder<Object[]> parameters = new ImmutableList.Builder<>();
        parameters.add(new Object[] {ORC, NONE, OptionalInt.empty()});
        parameters.add(new Object[] {DWRF, ZSTD, OptionalInt.of(7)});
        parameters.add(new Object[] {DWRF, ZSTD, OptionalInt.empty()});
        parameters.add(new Object[] {DWRF, ZLIB, OptionalInt.of(5)});
        parameters.add(new Object[] {DWRF, ZLIB, OptionalInt.empty()});
        return parameters.build().toArray(new Object[0][]);
    }

    @Test(dataProvider = "compressionLevels")
    public void testWriteOutputStreamsInOrder(OrcEncoding encoding, CompressionKind kind, OptionalInt level)
            throws IOException
    {
        testStreamOrder(encoding, kind, level, new StreamSizeLayoutFactory(), () -> new Consumer<Stream>()
        {
            int size;

            @Override
            public void accept(Stream stream)
            {
                if (!isIndexStream(stream)) {
                    assertGreaterThanOrEqual(stream.getLength(), size, stream.toString());
                    size = stream.getLength();
                }
            }
        });
    }

    @Test(dataProvider = "compressionLevels")
    public void testOutputStreamsByColumnSize(OrcEncoding encoding, CompressionKind kind, OptionalInt level)
            throws IOException
    {
        Map<Integer, Integer> nodeSizes = new LinkedHashMap<>();
        testStreamOrder(encoding, kind, level, new ColumnSizeLayoutFactory(), () -> stream -> {
            if (!isIndexStream(stream)) {
                int node = stream.getColumn();
                int oldSize = nodeSizes.computeIfAbsent(node, (c) -> 0);
                nodeSizes.put(node, oldSize + stream.getLength());
            }
        });

        List<Integer> actual = ImmutableList.copyOf(nodeSizes.values());
        List<Integer> expected = actual.stream().sorted(Comparator.reverseOrder()).collect(Collectors.toList());
        assertEquals(actual, expected);
    }

    private void testStreamOrder(OrcEncoding encoding, CompressionKind kind, OptionalInt level, StreamLayoutFactory streamLayoutFactory, Supplier<Consumer<Stream>> streamConsumerFactory)
            throws IOException
    {
        OrcWriterOptions orcWriterOptions = OrcWriterOptions.builder()
                .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                        .withStripeMinSize(new DataSize(0, MEGABYTE))
                        .withStripeMaxSize(new DataSize(32, MEGABYTE))
                        .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                        .build())
                .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                .withDictionaryMaxMemory(new DataSize(32, MEGABYTE))
                .withCompressionLevel(level)
                .withStreamLayoutFactory(streamLayoutFactory)
                .build();
        for (OrcWriteValidationMode validationMode : OrcWriteValidationMode.values()) {
            TempFile tempFile = new TempFile();
            OrcWriter writer = new OrcWriter(
                    new OutputStreamDataSink(new FileOutputStream(tempFile.getFile())),
                    ImmutableList.of("test1", "test2", "test3", "test4", "test5"),
                    ImmutableList.of(VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR),
                    encoding,
                    kind,
                    Optional.empty(),
                    NO_ENCRYPTION,
                    orcWriterOptions,
                    ImmutableMap.of(),
                    HIVE_STORAGE_TIME_ZONE,
                    true,
                    validationMode,
                    NOOP_WRITER_STATS);

            // write down some data with unsorted streams
            String[] data = new String[] {"a", "bbbbb", "ccc", "dd", "eeee"};
            Block[] blocks = new Block[data.length];
            int entries = 65536;
            BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, entries);
            for (int i = 0; i < data.length; i++) {
                byte[] bytes = data[i].getBytes();
                for (int j = 0; j < entries; j++) {
                    // force to write different data
                    bytes[0] = (byte) ((bytes[0] + 1) % 128);
                    blockBuilder.writeBytes(Slices.wrappedBuffer(bytes, 0, bytes.length), 0, bytes.length);
                    blockBuilder.closeEntry();
                }
                blocks[i] = blockBuilder.build();
                blockBuilder = blockBuilder.newBlockBuilderLike(null);
            }

            writer.write(new Page(blocks));
            writer.close();

            for (StripeFooter stripeFooter : OrcTester.getStripes(tempFile.getFile(), encoding)) {
                Consumer<Stream> streamConsumer = streamConsumerFactory.get();
                boolean dataStreamStarted = false;
                for (Stream stream : stripeFooter.getStreams()) {
                    if (isIndexStream(stream)) {
                        assertFalse(dataStreamStarted);
                        continue;
                    }
                    dataStreamStarted = true;
                    streamConsumer.accept(stream);
                }
            }
        }
    }

    @Test(expectedExceptions = IOException.class, expectedExceptionsMessageRegExp = "Dummy exception from mocked instance")
    public void testVerifyNoIllegalStateException()
            throws IOException
    {
        OrcWriter writer = new OrcWriter(
                new MockDataSink(),
                ImmutableList.of("test1"),
                ImmutableList.of(VARCHAR),
                ORC,
                NONE,
                Optional.empty(),
                NO_ENCRYPTION,
                OrcWriterOptions.builder()
                        .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                                .withStripeMinSize(new DataSize(0, MEGABYTE))
                                .withStripeMaxSize(new DataSize(32, MEGABYTE))
                                .withStripeMaxRowCount(10)
                                .build())
                        .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                        .withDictionaryMaxMemory(new DataSize(32, MEGABYTE))
                        .build(),
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                null,
                NOOP_WRITER_STATS);

        int entries = 65536;
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, entries);
        byte[] bytes = "dummyString".getBytes();
        for (int j = 0; j < entries; j++) {
            // force to write different data
            bytes[0] = (byte) ((bytes[0] + 1) % 128);
            blockBuilder.writeBytes(Slices.wrappedBuffer(bytes, 0, bytes.length), 0, bytes.length);
            blockBuilder.closeEntry();
        }
        Block[] blocks = new Block[] {blockBuilder.build()};

        try {
            // Throw IOException after first flush
            writer.write(new Page(blocks));
        }
        catch (IOException e) {
            writer.close();
        }
    }

    @Test
    public void testHllSnapshotsWrittenToFooter()
            throws IOException
    {
        int rowCount = 500;
        int distinctLongs = 12;
        int distinctStrings = 7;
        int distinctTimestamps = 9;

        TempFile tempFile = new TempFile();
        writeHllTestColumns(tempFile, true, rowCount, distinctLongs, distinctStrings, distinctTimestamps);

        Footer footer = OrcTester.getFileMetadata(tempFile.getFile(), DWRF).getFooter();
        List<OrcType> orcTypes = footer.getTypes();
        Slice serialized = footer.getUserMetadata().get(HLL_USER_METADATA_KEY);
        assertTrue(serialized != null, "footer must contain the HLL snapshot metadata");

        List<Optional<HyperLogLog>> snapshots = deserialize(serialized.toByteBuffer());
        // The snapshot list must align 1:1 with the OrcType nodes (pre-order,
        // root at index 0).
        assertEquals(snapshots.size(), orcTypes.size());

        // Column order: longs, strings, bools, array<bigint>, ts.
        OrcType root = orcTypes.get(0);
        int longNode = root.getFieldTypeIndex(0);
        int stringNode = root.getFieldTypeIndex(1);
        int timestampNode = root.getFieldTypeIndex(4);

        // Only the child-of-root numeric, string, and timestamp columns collect a
        // sketch; the root, the boolean column, the list, and its nested element
        // are all empty.
        for (int node = 0; node < snapshots.size(); node++) {
            boolean expectedPresent = node == longNode || node == stringNode || node == timestampNode;
            assertEquals(
                    snapshots.get(node).isPresent(),
                    expectedPresent,
                    "unexpected presence at node " + node);
        }

        assertCardinalityWithinTolerance(snapshots.get(longNode).get().cardinality(), distinctLongs);
        assertCardinalityWithinTolerance(snapshots.get(stringNode).get().cardinality(), distinctStrings);
        assertCardinalityWithinTolerance(snapshots.get(timestampNode).get().cardinality(), distinctTimestamps);
    }

    @Test
    public void testHllSnapshotsDisabledByDefault()
            throws IOException
    {
        TempFile tempFile = new TempFile();
        writeHllTestColumns(tempFile, false, 100, 5, 5, 5);

        Footer footer = OrcTester.getFileMetadata(tempFile.getFile(), DWRF).getFooter();
        assertFalse(
                footer.getUserMetadata().containsKey(HLL_USER_METADATA_KEY),
                "no HLL metadata should be emitted when collection is disabled");
    }

    // Sketches from separate files merge into the partition-level NDV the stats
    // publisher reports: two files with overlapping values, one column per
    // collected type, merged with HyperLogLog#mergeWith as the publisher does.
    @Test
    public void testHllSnapshotsMergeAcrossFiles()
            throws IOException
    {
        List<String> names = ImmutableList.of("id", "name", "score", "rating", "ts");
        // Distinct values in the union of both files, per column.
        long[] expectedUnion = {10_000, 5_000, 1_200, 120, 1_200};

        List<HyperLogLog> merged = new ArrayList<>();
        for (int part = 0; part < 2; part++) {
            try (TempFile tempFile = new TempFile()) {
                writeHllPartFile(tempFile, part);
                Footer footer = OrcTester.getFileMetadata(tempFile.getFile(), DWRF).getFooter();
                List<Optional<HyperLogLog>> snapshots =
                        deserialize(footer.getUserMetadata().get(HLL_USER_METADATA_KEY).toByteBuffer());
                OrcType root = footer.getTypes().get(0);
                for (int column = 0; column < names.size(); column++) {
                    Optional<HyperLogLog> sketch = snapshots.get(root.getFieldTypeIndex(column));
                    assertTrue(sketch.isPresent(), "no sketch for column " + names.get(column));
                    if (part == 0) {
                        merged.add(sketch.get());
                    }
                    else {
                        merged.get(column).mergeWith(sketch.get());
                    }
                }
            }
        }

        for (int column = 0; column < names.size(); column++) {
            assertCardinalityWithinTolerance(merged.get(column).cardinality(), expectedUnion[column]);
        }
    }

    @Test
    public void testHllSketchMemoryIsTracked()
            throws IOException
    {
        try (TempFile disabledFile = new TempFile(); TempFile enabledFile = new TempFile()) {
            OrcWriter disabled = createHllMemoryTestWriter(disabledFile, false);
            OrcWriter enabled = createHllMemoryTestWriter(enabledFile, true);
            BlockBuilder values = BIGINT.createBlockBuilder(null, 5_000);
            for (int i = 0; i < 5_000; i++) {
                BIGINT.writeLong(values, i);
            }
            Page page = new Page(values.build());
            disabled.write(page);
            enabled.write(page);

            // 5,000 distinct values make the sketch dense (2048 buckets), so the
            // enabled writer retains noticeably more.
            assertGreaterThanOrEqual(enabled.getRetainedBytes() - disabled.getRetainedBytes(), 1_000L);
            disabled.close();
            enabled.close();
        }
    }

    private static OrcWriter createHllMemoryTestWriter(TempFile tempFile, boolean hllCollectionEnabled)
            throws IOException
    {
        return new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(tempFile.getFile())),
                ImmutableList.of("id"),
                ImmutableList.of(BIGINT),
                DWRF,
                ZSTD,
                Optional.empty(),
                NO_ENCRYPTION,
                OrcWriterOptions.builder().withHllCollectionEnabled(hllCollectionEnabled).build(),
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                null,
                NOOP_WRITER_STATS);
    }

    // Writes 6,000 rows whose values overlap with the other part's, so each
    // column's union across both parts has a known distinct count.
    private static void writeHllPartFile(TempFile tempFile, int part)
            throws IOException
    {
        int rowCount = 6_000;
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, DOUBLE, REAL, TIMESTAMP);
        OrcWriterOptions options = OrcWriterOptions.builder()
                .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                        .withStripeMinSize(new DataSize(0, MEGABYTE))
                        .withStripeMaxSize(new DataSize(32, MEGABYTE))
                        .withStripeMaxRowCount(1_000)
                        .build())
                .withHllCollectionEnabled(true)
                .build();
        OrcWriter writer = new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(tempFile.getFile())),
                ImmutableList.of("id", "name", "score", "rating", "ts"),
                types,
                DWRF,
                ZSTD,
                Optional.empty(),
                NO_ENCRYPTION,
                options,
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                null,
                NOOP_WRITER_STATS);

        BlockBuilder ids = BIGINT.createBlockBuilder(null, rowCount);
        BlockBuilder names = VARCHAR.createBlockBuilder(null, rowCount);
        BlockBuilder scores = DOUBLE.createBlockBuilder(null, rowCount);
        BlockBuilder ratings = REAL.createBlockBuilder(null, rowCount);
        BlockBuilder timestamps = TIMESTAMP.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(ids, i + part * 4_000L);
            VARCHAR.writeSlice(names, Slices.utf8Slice("s" + (i % 3_000 + part * 2_000)));
            DOUBLE.writeDouble(scores, i % 700 + part * 500 + 0.25);
            REAL.writeLong(ratings, floatToRawIntBits(i % 80 + part * 40 + 0.5f));
            TIMESTAMP.writeLong(timestamps, (i % 900 + part * 300) * 1_000L + 250L);
        }
        writer.write(new Page(new Block[] {
                ids.build(), names.build(), scores.build(), ratings.build(), timestamps.build()}));
        writer.close();
    }

    // Fixed epoch-millis timestamps used to build the cross-writer golden
    // sketch. Kept in sync with /tmp generator GenTimestampGolden (its output is
    // checked in as timestamp_hll_golden.data). Includes pre-epoch instants
    // whose sub-second nanos wrap (e.g. -1 -> nanos 999_000_000) to exercise the
    // floorMod path in OrcWriter's timestamp hashing.
    private static final long[] TIMESTAMP_GOLDEN_MILLIS = {
            0L,
            1000L,
            1234L,
            1500L,
            999L,
            60000L,
            1_600_000_000_000L,
            1_600_000_000_250L,
            -1L,
            -1000L,
            -1500L,
            -86_400_000L,
            253_402_300_799_000L,
    };

    @Test
    public void testTimestampHllMatchesBbioGolden()
            throws IOException
    {
        // Write the timestamps through the real OrcWriter TIMESTAMP path, read
        // the sketch back from the footer, and assert its serialized bytes equal
        // the golden produced by bbio's writer (Hive Timestamp (time, nanos) ->
        // XxHash64 -> HyperLogLog). This proves the two writers emit
        // byte-identical timestamp sketches, so Spectrum can merge presto- and
        // bbio-written DWRF files in the same partition without NDV drift.
        TempFile tempFile = new TempFile();

        OrcWriterOptions options = OrcWriterOptions.builder()
                .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                        .withStripeMinSize(new DataSize(0, MEGABYTE))
                        .withStripeMaxSize(new DataSize(32, MEGABYTE))
                        .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                        .build())
                .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                .withDictionaryMaxMemory(new DataSize(32, MEGABYTE))
                .withHllCollectionEnabled(true)
                .build();

        OrcWriter writer = new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(tempFile.getFile())),
                ImmutableList.of("ts"),
                ImmutableList.of(TIMESTAMP),
                DWRF,
                ZLIB,
                Optional.empty(),
                NO_ENCRYPTION,
                options,
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                null,
                NOOP_WRITER_STATS);

        BlockBuilder timestampBuilder = TIMESTAMP.createBlockBuilder(null, TIMESTAMP_GOLDEN_MILLIS.length);
        for (long millis : TIMESTAMP_GOLDEN_MILLIS) {
            TIMESTAMP.writeLong(timestampBuilder, millis);
        }
        writer.write(new Page(timestampBuilder.build()));
        writer.close();

        Footer footer = OrcTester.getFileMetadata(tempFile.getFile(), DWRF).getFooter();
        List<Optional<HyperLogLog>> snapshots = deserialize(footer.getUserMetadata().get(HLL_USER_METADATA_KEY).toByteBuffer());
        // Nodes: 0 = root struct, 1 = ts column.
        HyperLogLog timestampSketch = snapshots.get(1).orElseThrow(() -> new AssertionError("timestamp sketch missing"));

        byte[] golden = toByteArray(getResource("timestamp_hll_golden.data"));
        assertEquals(timestampSketch.serialize().getBytes(), golden);
    }

    private static void writeHllTestColumns(TempFile tempFile, boolean hllCollectionEnabled, int rowCount, int distinctLongs, int distinctStrings, int distinctTimestamps)
            throws IOException
    {
        ArrayType arrayType = new ArrayType(BIGINT);
        List<Type> types = ImmutableList.of(BIGINT, VARCHAR, BOOLEAN, arrayType, TIMESTAMP);

        OrcWriterOptions options = OrcWriterOptions.builder()
                .withFlushPolicy(DefaultOrcWriterFlushPolicy.builder()
                        .withStripeMinSize(new DataSize(0, MEGABYTE))
                        .withStripeMaxSize(new DataSize(32, MEGABYTE))
                        .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                        .build())
                .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                .withDictionaryMaxMemory(new DataSize(32, MEGABYTE))
                .withHllCollectionEnabled(hllCollectionEnabled)
                .build();

        OrcWriter writer = new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(tempFile.getFile())),
                ImmutableList.of("longs", "strings", "bools", "arr", "ts"),
                types,
                DWRF,
                ZLIB,
                Optional.empty(),
                NO_ENCRYPTION,
                options,
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                null,
                NOOP_WRITER_STATS);

        BlockBuilder longBuilder = BIGINT.createBlockBuilder(null, rowCount);
        BlockBuilder stringBuilder = VARCHAR.createBlockBuilder(null, rowCount);
        BlockBuilder booleanBuilder = BOOLEAN.createBlockBuilder(null, rowCount);
        BlockBuilder arrayBuilder = arrayType.createBlockBuilder(null, rowCount);
        BlockBuilder timestampBuilder = TIMESTAMP.createBlockBuilder(null, rowCount);
        for (int i = 0; i < rowCount; i++) {
            BIGINT.writeLong(longBuilder, i % distinctLongs);
            VARCHAR.writeSlice(stringBuilder, Slices.utf8Slice("v" + (i % distinctStrings)));
            BOOLEAN.writeBoolean(booleanBuilder, i % 2 == 0);
            BlockBuilder elementBuilder = arrayBuilder.beginBlockEntry();
            BIGINT.writeLong(elementBuilder, i);
            BIGINT.writeLong(elementBuilder, i + 1);
            arrayBuilder.closeEntry();
            // Distinct timestamps, one full second apart, with a fixed sub-second
            // millisecond component to exercise the (millis, nanos) hash path.
            TIMESTAMP.writeLong(timestampBuilder, (i % distinctTimestamps) * 1000L + 250L);
        }

        Block[] blocks = new Block[] {
                longBuilder.build(),
                stringBuilder.build(),
                booleanBuilder.build(),
                arrayBuilder.build(),
                timestampBuilder.build(),
        };
        writer.write(new Page(blocks));
        writer.close();
    }

    private static void assertCardinalityWithinTolerance(long actual, long expected)
    {
        long tolerance = Math.max(1, expected / 10);
        assertTrue(
                actual >= expected - tolerance && actual <= expected + tolerance,
                String.format("cardinality %s not within 10%% of %s", actual, expected));
    }

    public static class MockDataSink
            implements DataSink
    {
        public MockDataSink()
        {
        }

        @Override
        public long size()
        {
            return -1L;
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return -1L;
        }

        @Override
        public void write(List<DataOutput> outputData)
                throws IOException
        {
            throw new IOException("Dummy exception from mocked instance");
        }

        @Override
        public void close()
        {
        }
    }
}
