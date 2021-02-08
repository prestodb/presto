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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Footer;
import com.facebook.presto.orc.metadata.Stream;
import com.facebook.presto.orc.metadata.StripeFooter;
import com.facebook.presto.orc.metadata.StripeInformation;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.StripeReader.isIndexStream;
import static com.facebook.presto.orc.TestingOrcPredicate.ORC_ROW_GROUP_SIZE;
import static com.facebook.presto.orc.TestingOrcPredicate.ORC_STRIPE_SIZE;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertFalse;

public class TestOrcWriter
{
    @DataProvider(name = "compressionLevels")
    public static Object[][] zstdCompressionLevels()
    {
        ImmutableList.Builder<Object[]> parameters = new ImmutableList.Builder<>();
        parameters.add(new Object[]{ORC, NONE, OptionalInt.empty()});
        parameters.add(new Object[]{DWRF, ZSTD, OptionalInt.of(7)});
        parameters.add(new Object[]{DWRF, ZSTD, OptionalInt.empty()});
        parameters.add(new Object[]{DWRF, ZLIB, OptionalInt.of(5)});
        parameters.add(new Object[]{DWRF, ZLIB, OptionalInt.empty()});
        return parameters.build().toArray(new Object[0][]);
    }

    @Test(dataProvider = "compressionLevels")
    public void testWriteOutputStreamsInOrder(OrcEncoding encoding, CompressionKind kind, OptionalInt level)
            throws IOException
    {
        OrcWriterOptions orcWriterOptions = new OrcWriterOptions()
                .withStripeMinSize(new DataSize(0, MEGABYTE))
                .withStripeMaxSize(new DataSize(32, MEGABYTE))
                .withStripeMaxRowCount(ORC_STRIPE_SIZE)
                .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                .withDictionaryMaxMemory(new DataSize(32, MEGABYTE))
                .withCompressionLevel(level);
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
                    new OrcWriterStats());

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

            // read the footer and verify the streams are ordered by size
            boolean isZstdJniCompressorEnabled = true;
            DataSize dataSize = new DataSize(1, MEGABYTE);
            OrcDataSource orcDataSource = new FileOrcDataSource(tempFile.getFile(), dataSize, dataSize, dataSize, true);
            Footer footer = new OrcReader(
                    orcDataSource,
                    encoding,
                    new StorageOrcFileTailSource(),
                    new StorageStripeMetadataSource(),
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    new OrcReaderOptions(
                            dataSize,
                            dataSize,
                            dataSize,
                            isZstdJniCompressorEnabled),
                    false,
                    NO_ENCRYPTION,
                    DwrfKeyProvider.EMPTY
            ).getFooter();

            int bufferSize = toIntExact(orcWriterOptions.getMaxCompressionBufferSize().toBytes());
            Optional<OrcDecompressor> decompressor = createOrcDecompressor(orcDataSource.getId(), kind, bufferSize, isZstdJniCompressorEnabled);

            for (StripeInformation stripe : footer.getStripes()) {
                // read the footer
                byte[] tailBuffer = new byte[toIntExact(stripe.getFooterLength())];
                orcDataSource.readFully(stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength(), tailBuffer);
                try (InputStream inputStream = new OrcInputStream(
                        orcDataSource.getId(),
                        new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT),
                        Slices.wrappedBuffer(tailBuffer).getInput(),
                        decompressor,
                        Optional.empty(),
                        new TestingHiveOrcAggregatedMemoryContext(),
                        tailBuffer.length)) {
                    StripeFooter stripeFooter = encoding.createMetadataReader().readStripeFooter(footer.getTypes(), inputStream);

                    int size = 0;
                    boolean dataStreamStarted = false;
                    for (Stream stream : stripeFooter.getStreams()) {
                        if (isIndexStream(stream)) {
                            assertFalse(dataStreamStarted);
                            continue;
                        }
                        dataStreamStarted = true;
                        // verify sizes in order
                        assertGreaterThanOrEqual(stream.getLength(), size);
                        size = stream.getLength();
                    }
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
                new OrcWriterOptions()
                        .withStripeMinSize(new DataSize(0, MEGABYTE))
                        .withStripeMaxSize(new DataSize(32, MEGABYTE))
                        .withStripeMaxRowCount(10)
                        .withRowGroupMaxRowCount(ORC_ROW_GROUP_SIZE)
                        .withDictionaryMaxMemory(new DataSize(32, MEGABYTE)),
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                null,
                new OrcWriterStats());

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
