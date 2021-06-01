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

import com.facebook.presto.orc.StripeReader.StripeId;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.DwrfStripeCache;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestOrcReaderDwrfStripeCaching
{
    private static final int READ_TAIL_SIZE = 1024 * 1024;
    private static final OrcDataSourceId TEST_DATA_SOURCE_ID = new OrcDataSourceId("test");

    @Test
    public void testBothAllStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_BOTH_AllStripes.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertTrue(optionalDwrfStripeCache.isPresent());
        DwrfStripeCache dwrfStripeCache = optionalDwrfStripeCache.get();

        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        assertEquals(stripes.size(), 4);

        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            for (int i = 0; i < 4; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCachePresent(dwrfStripeCache, file, stripe);
                assertStripeFooterCachePresent(dwrfStripeCache, file, stripe);
            }
        }
    }

    @Test
    public void testBothHalfStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_BOTH_HalfStripes.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertTrue(optionalDwrfStripeCache.isPresent());
        DwrfStripeCache dwrfStripeCache = optionalDwrfStripeCache.get();

        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        assertEquals(stripes.size(), 4);

        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            for (int i = 0; i < 2; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCachePresent(dwrfStripeCache, file, stripe);
                assertStripeFooterCachePresent(dwrfStripeCache, file, stripe);
            }

            for (int i = 2; i < 4; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCacheAbsent(dwrfStripeCache, stripe);
                assertStripeFooterCacheAbsent(dwrfStripeCache, stripe);
            }
        }
    }

    @Test
    public void testIndexAllStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_INDEX_AllStripes.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertTrue(optionalDwrfStripeCache.isPresent());
        DwrfStripeCache dwrfStripeCache = optionalDwrfStripeCache.get();

        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        assertEquals(stripes.size(), 4);

        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            for (int i = 0; i < 4; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCachePresent(dwrfStripeCache, file, stripe);
                assertStripeFooterCacheAbsent(dwrfStripeCache, stripe);
            }
        }
    }

    @Test
    public void testIndexHalfStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_INDEX_HalfStripes.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertTrue(optionalDwrfStripeCache.isPresent());
        DwrfStripeCache dwrfStripeCache = optionalDwrfStripeCache.get();

        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        assertEquals(stripes.size(), 4);

        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            for (int i = 0; i < 2; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCachePresent(dwrfStripeCache, file, stripe);
                assertStripeFooterCacheAbsent(dwrfStripeCache, stripe);
            }
            for (int i = 2; i < 4; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCacheAbsent(dwrfStripeCache, stripe);
                assertStripeFooterCacheAbsent(dwrfStripeCache, stripe);
            }
        }
    }

    @Test
    public void testFooterAllStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_FOOTER_AllStripes.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertTrue(optionalDwrfStripeCache.isPresent());
        DwrfStripeCache dwrfStripeCache = optionalDwrfStripeCache.get();

        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        assertEquals(stripes.size(), 4);

        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            for (int i = 0; i < 4; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCacheAbsent(dwrfStripeCache, stripe);
                assertStripeFooterCachePresent(dwrfStripeCache, file, stripe);
            }
        }
    }

    @Test
    public void testFooterHalfStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_FOOTER_HalfStripes.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertTrue(optionalDwrfStripeCache.isPresent());
        DwrfStripeCache dwrfStripeCache = optionalDwrfStripeCache.get();

        DwrfProto.Footer footer = readFileFooter(orcFile);
        List<DwrfProto.StripeInformation> stripes = footer.getStripesList();
        assertEquals(stripes.size(), 4);

        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            for (int i = 0; i < 2; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCacheAbsent(dwrfStripeCache, stripe);
                assertStripeFooterCachePresent(dwrfStripeCache, file, stripe);
            }
            for (int i = 2; i < 4; i++) {
                DwrfProto.StripeInformation stripe = stripes.get(i);
                assertStripeIndexCacheAbsent(dwrfStripeCache, stripe);
                assertStripeFooterCacheAbsent(dwrfStripeCache, stripe);
            }
        }
    }

    @Test
    public void testNoneAllStripes()
            throws IOException
    {
        File orcFile = getResourceFile("DwrfStripeCache_NONE.orc");
        Optional<DwrfStripeCache> optionalDwrfStripeCache = getDwrfStripeCache(orcFile);
        assertFalse(optionalDwrfStripeCache.isPresent());
    }

    private void assertStripeIndexCachePresent(DwrfStripeCache dwrfStripeCache, RandomAccessFile file, DwrfProto.StripeInformation stripe)
            throws IOException
    {
        StripeId stripeId = new StripeId(TEST_DATA_SOURCE_ID, stripe.getOffset());
        Optional<Slice> stripeIndexSlice = dwrfStripeCache.getIndexStreamsSlice(stripeId);
        assertTrue(stripeIndexSlice.isPresent());
        assertEquals(stripeIndexSlice.get().getBytes(), readBytes(file, stripe.getOffset(), stripe.getIndexLength()));
    }

    private void assertStripeIndexCacheAbsent(DwrfStripeCache dwrfStripeCache, DwrfProto.StripeInformation stripe)
    {
        StripeId stripeId = new StripeId(TEST_DATA_SOURCE_ID, stripe.getOffset());
        Optional<Slice> stripeIndexSlice = dwrfStripeCache.getIndexStreamsSlice(stripeId);
        assertFalse(stripeIndexSlice.isPresent());
    }

    private void assertStripeFooterCachePresent(DwrfStripeCache dwrfStripeCache, RandomAccessFile file, DwrfProto.StripeInformation stripe)
            throws IOException
    {
        StripeId stripeId = new StripeId(TEST_DATA_SOURCE_ID, stripe.getOffset());
        Optional<Slice> stripeFooterSlice = dwrfStripeCache.getStripeFooterSlice(stripeId, toIntExact(stripe.getFooterLength()));
        assertTrue(stripeFooterSlice.isPresent());
        long footerOffset = stripe.getOffset() + stripe.getIndexLength() + stripe.getDataLength();
        assertEquals(stripeFooterSlice.get().getBytes(), readBytes(file, footerOffset, stripe.getFooterLength()));
    }

    private void assertStripeFooterCacheAbsent(DwrfStripeCache dwrfStripeCache, DwrfProto.StripeInformation stripe)
    {
        StripeId stripeId = new StripeId(TEST_DATA_SOURCE_ID, stripe.getOffset());
        Optional<Slice> stripeFooterSlice = dwrfStripeCache.getStripeFooterSlice(stripeId, toIntExact(stripe.getFooterLength()));
        assertFalse(stripeFooterSlice.isPresent());
    }

    static File getResourceFile(String fileName)
    {
        String resourceName = "dwrf_stripe_cache/" + fileName;
        return new File(getResource(resourceName).getFile());
    }

    private Optional<DwrfStripeCache> getDwrfStripeCache(File orcFile)
            throws IOException
    {
        CapturingStripeMetadataSourceFactory stripeMetadataSourceFactory = new CapturingStripeMetadataSourceFactory();
        OrcDataSource orcDataSource = new FileOrcDataSource(
                orcFile,
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                new DataSize(1, MEGABYTE),
                true);
        new OrcReader(
                orcDataSource,
                DWRF,
                new StorageOrcFileTailSource(READ_TAIL_SIZE, true),
                stripeMetadataSourceFactory,
                NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                OrcReaderTestingUtils.createDefaultTestConfig(),
                false,
                NO_ENCRYPTION,
                DwrfKeyProvider.EMPTY);
        return stripeMetadataSourceFactory.getDwrfStripeCache();
    }

    static DwrfProto.Footer readFileFooter(File orcFile)
            throws IOException
    {
        try (RandomAccessFile file = new RandomAccessFile(orcFile, "r")) {
            // read postscript size
            file.seek(file.length() - 1);
            int postScriptSize = file.read() & 0xff;

            // read postscript
            long postScriptPos = file.length() - postScriptSize - 1;
            byte[] postScriptBytes = readBytes(file, postScriptPos, postScriptSize);
            CodedInputStream postScriptInput = CodedInputStream.newInstance(postScriptBytes, 0, postScriptSize);
            DwrfProto.PostScript postScript = DwrfProto.PostScript.parseFrom(postScriptInput);

            // read footer
            long footerPos = postScriptPos - postScript.getFooterLength();
            int footerLen = toIntExact(postScript.getFooterLength());
            byte[] footerBytes = readBytes(file, footerPos, postScript.getFooterLength());

            int compressionBufferSize = toIntExact(postScript.getCompressionBlockSize());
            OrcDataSourceId dataSourceId = new OrcDataSourceId(orcFile.getName());
            Optional<OrcDecompressor> decompressor = OrcDecompressor.createOrcDecompressor(dataSourceId, ZLIB, compressionBufferSize);
            InputStream footerInputStream = new OrcInputStream(
                    dataSourceId,
                    new SharedBuffer(NOOP_ORC_LOCAL_MEMORY_CONTEXT),
                    Slices.wrappedBuffer(footerBytes).slice(0, footerLen).getInput(),
                    decompressor,
                    Optional.empty(),
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    footerLen);

            return DwrfProto.Footer.parseFrom(footerInputStream);
        }
    }

    private static byte[] readBytes(RandomAccessFile file, long offset, long length)
            throws IOException
    {
        byte[] buf = new byte[toIntExact(length)];
        file.seek(offset);
        file.readFully(buf, 0, buf.length);
        return buf;
    }

    private static class CapturingStripeMetadataSourceFactory
            implements StripeMetadataSourceFactory
    {
        private final StorageStripeMetadataSource source = new StorageStripeMetadataSource();
        private Optional<DwrfStripeCache> dwrfStripeCache;

        @Override
        public StripeMetadataSource create(Optional<DwrfStripeCache> dwrfStripeCache)
        {
            this.dwrfStripeCache = dwrfStripeCache;
            return source;
        }

        public Optional<DwrfStripeCache> getDwrfStripeCache()
        {
            return dwrfStripeCache;
        }
    }
}
