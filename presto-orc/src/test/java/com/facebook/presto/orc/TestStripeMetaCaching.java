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
import com.facebook.presto.orc.array.Arrays;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.facebook.presto.orc.protobuf.MessageLite;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import io.airlift.slice.OutputStreamSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.facebook.airlift.testing.Assertions.assertGreaterThanOrEqual;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.DWRF;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnsPresto;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.joda.time.DateTimeZone.UTC;
import static org.testng.Assert.assertEquals;

public class TestStripeMetaCaching
{
    private static final int READ_TAIL_SIZE = 1024 * 1024; // comes from StorageOrcFileTailSource
    private static final int ROW_COUNT = 100_000;
    private static final DataSize ONE_MB = new DataSize(1, MEGABYTE);
    private final CompressionKind compressionKind = ZLIB;
    private final TempFile tempFile = new TempFile();
    private final TempFile fileWithFooter = new TempFile();
    private final TempFile fileWithIndex = new TempFile();
    private final TempFile fileWithBoth = new TempFile();
    private final OrcDataSourceId dataSourceId = new OrcDataSourceId(tempFile.toString());
    private List<DwrfProto.StripeInformation> stripes;
    private int splitStartRow;
    private long splitOffset;
    private List<String> values1;
    private List<String> values2;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        Random rnd = new Random();
        values1 = Stream.generate(() -> randomString(rnd)).limit(ROW_COUNT).collect(toImmutableList());
        values2 = Stream.generate(() -> randomString(rnd)).limit(ROW_COUNT).collect(toImmutableList());

        writeOrcColumnsPresto(tempFile.getFile(),
                DWRF,
                compressionKind,
                new OrcWriterOptions().withStripeMaxRowCount(25_000),
                Optional.empty(),
                ImmutableList.of(VARCHAR, VARCHAR),
                ImmutableList.of(values1, values2),
                new OrcWriterStats());

        rewriteFile();
        setSplitInfo();
    }

    private void setSplitInfo()
    {
        int splitPoint = stripes.size() / 2;
        for (int i = 0; i < splitPoint; i++) {
            splitStartRow += stripes.get(i).getNumberOfRows();
        }
        splitOffset = stripes.get(splitPoint).getOffset();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        tempFile.close();
        fileWithFooter.close();
        fileWithIndex.close();
        fileWithBoth.close();
    }

    @Test
    public void testStripeFooterCache()
            throws IOException
    {
        TestingOrcDataSource dataSource = new TestingOrcDataSource(
                new FileOrcDataSource(fileWithFooter.getFile(), ONE_MB, ONE_MB, ONE_MB, true));

        assertFileContent(dataSource, 0, 0, dataSource.getSize());

        long fileTailOffset = fileWithFooter.getFile().length() - READ_TAIL_SIZE;
        validateNoCachedDataRead(dataSource.getReadRanges(), fileTailOffset, this::getStripeFooterRanges);
    }

    @Test
    public void testStripeFooterCacheWithSplit()
            throws IOException
    {
        TestingOrcDataSource dataSource = new TestingOrcDataSource(
                new FileOrcDataSource(fileWithFooter.getFile(), ONE_MB, ONE_MB, ONE_MB, true));

        assertFileContent(dataSource, splitStartRow, splitOffset, dataSource.getSize() - splitOffset);

        long fileTailOffset = fileWithFooter.getFile().length() - READ_TAIL_SIZE;
        validateNoCachedDataRead(dataSource.getReadRanges(), fileTailOffset, this::getStripeFooterRanges);
    }

    @Test
    public void testStripeIndexCache()
            throws IOException
    {
        TestingOrcDataSource dataSource = new TestingOrcDataSource(
                new FileOrcDataSource(fileWithIndex.getFile(), ONE_MB, ONE_MB, ONE_MB, true));

        assertFileContent(dataSource, 0, 0, dataSource.getSize());

        long fileTailOffset = fileWithIndex.getFile().length() - READ_TAIL_SIZE;
        validateNoCachedDataRead(dataSource.getReadRanges(), fileTailOffset, this::getStripeIndexRanges);
    }

    @Test
    public void testStripeIndexCacheWithSplit()
            throws IOException
    {
        TestingOrcDataSource dataSource = new TestingOrcDataSource(
                new FileOrcDataSource(fileWithIndex.getFile(), ONE_MB, ONE_MB, ONE_MB, true));

        assertFileContent(dataSource, splitStartRow, splitOffset, dataSource.getSize() - splitOffset);

        long fileTailOffset = fileWithIndex.getFile().length() - READ_TAIL_SIZE;
        validateNoCachedDataRead(dataSource.getReadRanges(), fileTailOffset, this::getStripeIndexRanges);
    }

    @Test
    public void testStripeIndexAndFooterCache()
            throws IOException
    {
        TestingOrcDataSource dataSource = new TestingOrcDataSource(
                new FileOrcDataSource(fileWithBoth.getFile(), ONE_MB, ONE_MB, ONE_MB, true));

        assertFileContent(dataSource, 0, 0, dataSource.getSize());

        long fileTailOffset = fileWithBoth.getFile().length() - READ_TAIL_SIZE;
        validateNoCachedDataRead(dataSource.getReadRanges(), fileTailOffset, this::getStripeIndexAndFooterRanges);
    }

    @Test
    public void testStripeIndexAndFooterCacheWithSplit()
            throws IOException
    {
        TestingOrcDataSource dataSource = new TestingOrcDataSource(
                new FileOrcDataSource(fileWithBoth.getFile(), ONE_MB, ONE_MB, ONE_MB, true));

        assertFileContent(dataSource, splitStartRow, splitOffset, dataSource.getSize() - splitOffset);

        long fileTailOffset = fileWithBoth.getFile().length() - READ_TAIL_SIZE;
        validateNoCachedDataRead(dataSource.getReadRanges(), fileTailOffset, this::getStripeIndexAndFooterRanges);
    }

    private void validateNoCachedDataRead(List<DiskRange> readRanges, long fileTailOffset, Function<DwrfProto.StripeInformation, List<DiskRange>> stripeRangeProducer)
    {
        DiskRange range1;
        DiskRange range2;

        // go over all pairs of read+stripe ranges,
        for (DwrfProto.StripeInformation stripe : stripes) {
            for (DiskRange stripeRange : stripeRangeProducer.apply(stripe)) {
                for (DiskRange readRange : readRanges) {
                    // skip read ranges that fall under the file tail read by the StorageOrcFileTailSource
                    if (readRange.getEnd() >= fileTailOffset) {
                        continue;
                    }

                    // sort range pairs so that range1 comes first
                    if (readRange.getOffset() <= stripeRange.getOffset()) {
                        range1 = readRange;
                        range2 = stripeRange;
                    }
                    else {
                        range1 = stripeRange;
                        range2 = readRange;
                    }

                    // check that start of the range2 doesn't fall under the range1
                    assertGreaterThanOrEqual(range2.getOffset(), range1.getEnd());
                }
            }
        }
    }

    private void assertFileContent(OrcDataSource dataSource, int startRow, long splitOffset, long splitLength)
            throws IOException
    {
        OrcReaderOptions readerOptions = new OrcReaderOptions(ONE_MB, ONE_MB, ONE_MB, ImmutableMap.of(OrcReaderFeature.STRIPE_META_CACHE, true));
        OrcReader orcReader = new OrcReader(
                dataSource,
                OrcEncoding.DWRF,
                new StorageOrcFileTailSource(),
                new StorageStripeMetadataSource(),
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                readerOptions,
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY);

        OrcSelectiveRecordReader recordReader = orcReader.createSelectiveRecordReader(
                ImmutableMap.of(0, VARCHAR, 1, VARCHAR),
                ImmutableList.of(0, 1),
                ImmutableMap.of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                OrcPredicate.TRUE,
                splitOffset,
                splitLength,
                UTC, // arbitrary
                true,
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                Optional.empty(),
                INITIAL_BATCH_SIZE);

        Page page;
        int recordCnt = startRow;
        while ((page = recordReader.getNextPage()) != null) {
            Block block1 = page.getBlock(0);
            Block block2 = page.getBlock(1);
            for (int i = 0; i < page.getPositionCount(); i++) {
                Slice slice1 = block1.getSlice(i, 0, block1.getSliceLength(i));
                Slice slice2 = block2.getSlice(i, 0, block2.getSliceLength(i));
                assertEquals(slice1.toString(UTF_8), values1.get(recordCnt));
                assertEquals(slice2.toString(UTF_8), values2.get(recordCnt));
                recordCnt++;
            }
        }
        assertEquals(recordCnt, ROW_COUNT);
    }

    // Rewrites the file and appends stripe meta caches according to the mode.
    // Remove this method when OrcWriter gets support for the stripe metadata cache.
    private void rewriteFile()
            throws IOException
    {
        byte[] buf = new byte[1024];
        ByteArrayOutputStream footerCache = new ByteArrayOutputStream();
        ByteArrayOutputStream indexCache = new ByteArrayOutputStream();
        ByteArrayOutputStream bothCache = new ByteArrayOutputStream();

        try (RandomAccessFile file = new RandomAccessFile(tempFile.getFile(), "r")) {
            // read postscript size
            file.seek(file.length() - 1);
            int postScriptSize = file.read() & 0xff;

            // read postscript
            long postScriptPos = file.length() - postScriptSize - 1;
            file.seek(postScriptPos);
            readBytes(file, buf, postScriptSize);

            CodedInputStream postScriptInput = CodedInputStream.newInstance(buf, 0, postScriptSize);
            DwrfProto.PostScript postScript = DwrfProto.PostScript.parseFrom(postScriptInput);

            // assume that caching in not supported yet
            assertEquals(postScript.getCacheSize(), 0);

            // read footer
            long footerPos = postScriptPos - postScript.getFooterLength();
            file.seek(footerPos);

            int footerLen = toIntExact(postScript.getFooterLength());
            buf = readBytes(file, buf, footerLen);

            int compressionBufferSize = toIntExact(postScript.getCompressionBlockSize());
            Optional<OrcDecompressor> decompressor = OrcDecompressor.createOrcDecompressor(dataSourceId, compressionKind, compressionBufferSize);
            InputStream footerInputStream = new OrcInputStream(
                    dataSourceId,
                    new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT),
                    Slices.wrappedBuffer(buf).slice(0, footerLen).getInput(),
                    decompressor,
                    Optional.empty(),
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    footerLen);

            DwrfProto.Footer footer = DwrfProto.Footer.parseFrom(footerInputStream);

            // check that the file has at least two stripes
            assertGreaterThanOrEqual(footer.getStripesCount(), 2);

            // collect binary versions of stripe indexes and stripe footers for
            // different versions of the metadata cache
            List<Integer> footerOffsets = new ArrayList<>();
            List<Integer> indexOffsets = new ArrayList<>();
            List<Integer> bothOffsets = new ArrayList<>();

            this.stripes = footer.getStripesList();
            for (DwrfProto.StripeInformation stripe : stripes) {
                // read & cache stripe index
                file.seek(stripe.getOffset());
                int stripeIndexLen = toIntExact(stripe.getIndexLength());
                buf = readBytes(file, buf, stripeIndexLen);

                bothOffsets.add(bothCache.size());
                bothCache.write(buf, 0, stripeIndexLen);

                indexOffsets.add(indexCache.size());
                indexCache.write(buf, 0, stripeIndexLen);

                // read & cache stripe footer
                file.seek(stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength());
                int stripeFooterLen = toIntExact(stripe.getFooterLength());
                buf = readBytes(file, buf, stripeFooterLen);

                footerOffsets.add(footerCache.size());
                footerCache.write(buf, 0, stripeFooterLen);

                bothOffsets.add(bothCache.size());
                bothCache.write(buf, 0, stripeFooterLen);
            }

            // collect the final size of the data to be able to calculate the size
            // of the last cached piece using offsets
            indexOffsets.add(indexCache.size());
            footerOffsets.add(footerCache.size());
            bothOffsets.add(bothCache.size());

            // create new files with the caches
            try (OutputStream fileWithFooterOut = new FileOutputStream(fileWithFooter.getFile());
                    OutputStream fileWithIndexOut = new FileOutputStream(fileWithIndex.getFile());
                    OutputStream fileWithBothOut = new FileOutputStream(fileWithBoth.getFile())) {
                // copy everything from the beginning all the way to the file tail
                file.seek(0);
                buf = readBytes(file, buf, toIntExact(footerPos));
                fileWithFooterOut.write(buf, 0, toIntExact(footerPos));
                fileWithIndexOut.write(buf, 0, toIntExact(footerPos));
                fileWithBothOut.write(buf, 0, toIntExact(footerPos));

                // add cached data and new footer+tail
                appendOrcFileTailWithCache(fileWithFooterOut, footer, postScript, compressionBufferSize, DwrfProto.StripeCacheMode.FOOTER, footerCache.toByteArray(), footerOffsets);
                appendOrcFileTailWithCache(fileWithIndexOut, footer, postScript, compressionBufferSize, DwrfProto.StripeCacheMode.INDEX, indexCache.toByteArray(), indexOffsets);
                appendOrcFileTailWithCache(fileWithBothOut, footer, postScript, compressionBufferSize, DwrfProto.StripeCacheMode.BOTH, bothCache.toByteArray(), bothOffsets);
            }
        }
    }

    private void appendOrcFileTailWithCache(OutputStream outputStream,
            DwrfProto.Footer footer,
            DwrfProto.PostScript postScript,
            int compressionBufferSize,
            DwrfProto.StripeCacheMode cacheMode,
            byte[] cache,
            List<Integer> offsets)
            throws IOException
    {
        // write metadata cache
        outputStream.write(cache);

        // write compressed file footer
        CompressionParameters compressionParameters = new CompressionParameters(compressionKind, OptionalInt.empty(), compressionBufferSize);
        OrcOutputBuffer compressedOut = new OrcOutputBuffer(compressionParameters, Optional.empty());

        DwrfProto.Footer newFooter = footer.toBuilder().addAllStripeCacheOffsets(offsets).build();
        newFooter.writeTo(compressedOut);
        compressedOut.flush();
        OutputStreamSliceOutput sliceOutput = new OutputStreamSliceOutput(outputStream);
        int footerLen = compressedOut.writeDataTo(sliceOutput);
        sliceOutput.flush();

        // write uncompressed postscript
        DwrfProto.PostScript newPostScript = postScript.toBuilder().setCacheSize(cache.length).setCacheMode(cacheMode).setFooterLength(footerLen).build();
        int postScriptLen = writeProtobufObject(outputStream, newPostScript);

        // write postscript size
        outputStream.write(postScriptLen & 0xff);
    }

    private static int writeProtobufObject(OutputStream output, MessageLite object)
            throws IOException
    {
        CountingOutputStream countingOutput = new CountingOutputStream(output);
        object.writeTo(countingOutput);
        return toIntExact(countingOutput.getCount());
    }

    // read bytes into a reusable buffer which can be grown when needed
    private static byte[] readBytes(RandomAccessFile file, byte[] buf, int length)
            throws IOException
    {
        buf = Arrays.ensureCapacity(buf, length);
        assertEquals(file.read(buf, 0, length), length);
        return buf;
    }

    private String randomString(Random rnd)
    {
        byte[] bytes = new byte[Math.max(64, rnd.nextInt(128))];
        rnd.nextBytes(bytes);
        return new String(bytes, UTF_8);
    }

    private List<DiskRange> getStripeFooterRanges(DwrfProto.StripeInformation stripe)
    {
        long offset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        DiskRange diskRange = new DiskRange(offset, toIntExact(stripe.getFooterLength()));
        return ImmutableList.of(diskRange);
    }

    private List<DiskRange> getStripeIndexRanges(DwrfProto.StripeInformation stripe)
    {
        long offset = stripe.getOffset();
        DiskRange diskRange = new DiskRange(offset, toIntExact(stripe.getIndexLength()));
        return ImmutableList.of(diskRange);
    }

    private List<DiskRange> getStripeIndexAndFooterRanges(DwrfProto.StripeInformation stripe)
    {
        long indexOffset = stripe.getOffset();
        long footerOffset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        DiskRange stripeIndexRange = new DiskRange(indexOffset, toIntExact(stripe.getIndexLength()));
        DiskRange stripeFooterRange = new DiskRange(footerOffset, toIntExact(stripe.getFooterLength()));
        return ImmutableList.of(stripeIndexRange, stripeFooterRange);
    }
}
