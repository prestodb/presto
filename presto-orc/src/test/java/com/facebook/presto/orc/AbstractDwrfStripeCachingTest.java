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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.metadata.DwrfStripeCacheMode;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.CodedInputStream;
import com.facebook.presto.orc.stream.OrcInputStream;
import com.facebook.presto.orc.stream.SharedBuffer;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.testng.annotations.DataProvider;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.NoopOrcLocalMemoryContext.NOOP_ORC_LOCAL_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.createOrcWriter;
import static com.facebook.presto.orc.metadata.CompressionKind.ZLIB;
import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.FOOTER;
import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX;
import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static com.google.common.io.Resources.getResource;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.Math.toIntExact;
import static org.testng.Assert.assertEquals;

public class AbstractDwrfStripeCachingTest
{
    private TempFile bothAllStripesFile = writeOrcFile(INDEX_AND_FOOTER, DataSize.valueOf("8MB"));
    private TempFile bothHalfStripesFiles = rewriteFile(bothAllStripesFile, INDEX_AND_FOOTER);
    private TempFile indexAllStripesFile = writeOrcFile(INDEX, DataSize.valueOf("8MB"));
    private TempFile indexHalfStripesFile = rewriteFile(indexAllStripesFile, INDEX);
    private TempFile footerAllStripesFile = writeOrcFile(FOOTER, DataSize.valueOf("8MB"));
    private TempFile footerHalfStripesFile = rewriteFile(footerAllStripesFile, FOOTER);
    private TempFile noneAllStripes = writeOrcFile(DwrfStripeCacheMode.NONE, DataSize.valueOf("8MB"));

    @DataProvider(name = "Stripe cache for ALL stripes with mode BOTH")
    public Object[][] bothAllStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_BOTH_AllStripes.orc"), bothAllStripesFile.getFile());
    }

    @DataProvider(name = "Stripe cache for HALF stripes with mode BOTH")
    public Object[][] bothHalfStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_BOTH_HalfStripes.orc"), bothHalfStripesFiles.getFile());
    }

    @DataProvider(name = "Stripe cache for ALL stripes with mode INDEX")
    public Object[][] indexAllStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_INDEX_AllStripes.orc"), indexAllStripesFile.getFile());
    }

    @DataProvider(name = "Stripe cache for HALF stripes with mode INDEX")
    public Object[][] indexHalfStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_INDEX_HalfStripes.orc"), indexHalfStripesFile.getFile());
    }

    @DataProvider(name = "Stripe cache for ALL stripes with mode FOOTER")
    public Object[][] footerAllStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_FOOTER_AllStripes.orc"), footerAllStripesFile.getFile());
    }

    @DataProvider(name = "Stripe cache for HALF stripes with mode FOOTER")
    public Object[][] footerHalfStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_FOOTER_HalfStripes.orc"), footerHalfStripesFile.getFile());
    }

    @DataProvider(name = "Stripe cache with mode NONE")
    public Object[][] noneAllStripesFilesProvider()
    {
        return toArray(getResourceFile("DwrfStripeCache_NONE.orc"), noneAllStripes.getFile());
    }

    static File getResourceFile(String fileName)
    {
        String resourceName = "dwrf_stripe_cache/" + fileName;
        return new File(getResource(resourceName).getFile());
    }

    private static Object[][] toArray(File file1, File file2)
    {
        return new Object[][] {{file1}, {file2}};
    }

    /**
     * Create an ORC file in a way that the stripe cache would only have data for
     * half of the stripes.
     */
    static TempFile rewriteFile(TempFile srcFile, DwrfStripeCacheMode mode)
    {
        DwrfProto.Footer footer = readFileFooter(srcFile.getFile());
        assertEquals(footer.getStripesCount(), 4);

        // calculate stripe cache size to fit data exactly for half (2) stripes
        long size = 0;
        for (int i = 0; i < 2; i++) {
            DwrfProto.StripeInformation stripe = footer.getStripes(i);
            if (mode.hasFooter()) {
                size += stripe.getFooterLength();
            }
            if (mode.hasIndex()) {
                size += stripe.getIndexLength();
            }
        }

        // now create the same file but with the specifically configured cache
        // size to fit data only for two stripes
        return writeOrcFile(mode, new DataSize(size, BYTE));
    }

    static TempFile writeOrcFile(DwrfStripeCacheMode cacheMode, DataSize cacheMaxSize)
    {
        OrcWriterOptions writerOptions = OrcWriterOptions.builder()
                .withDwrfStripeCacheEnabled(true)
                .withDwrfStripeCacheMode(cacheMode)
                .withDwrfStripeCacheMaxSize(cacheMaxSize)
                .withStripeMaxRowCount(100)
                .build();
        TempFile file = new TempFile();
        writeOrcFile(file.getFile(), writerOptions);
        return file;
    }

    /**
     * Create a file with 3 INT columns, 4 stripes with 100 rows each with the
     * following values:
     * Column 0: row number
     * Column 1: Integer.MAX_VALUE
     * Column 2: row number * 10
     */
    static void writeOrcFile(File outputFile, OrcWriterOptions writerOptions)
    {
        try {
            WriterStats stats = new OrcWriterStats();
            Type type = INTEGER;
            List<Type> types = ImmutableList.of(type, type, type);
            OrcWriter writer = createOrcWriter(outputFile, DWRF, ZLIB, Optional.empty(), types, writerOptions, stats);
            int cnt = 0;

            // write 4 stripes with 100 values each
            for (int stripe = 0; stripe < 4; stripe++) {
                BlockBuilder[] blockBuilders = new BlockBuilder[3];
                for (int i = 0; i < blockBuilders.length; i++) {
                    blockBuilders[i] = type.createBlockBuilder(null, 100);
                }

                for (int row = 0; row < 100; row++) {
                    blockBuilders[0].writeInt(cnt);
                    blockBuilders[1].writeInt(Integer.MAX_VALUE);
                    blockBuilders[2].writeInt(cnt * 10);
                    cnt++;
                }

                Block[] blocks = new Block[blockBuilders.length];
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = blockBuilders[i].build();
                }
                writer.write(new Page(blocks));
            }

            writer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static DwrfProto.Footer readFileFooter(File orcFile)
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
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static byte[] readBytes(RandomAccessFile file, long offset, long length)
            throws IOException
    {
        byte[] buf = new byte[toIntExact(length)];
        file.seek(offset);
        file.readFully(buf, 0, buf.length);
        return buf;
    }
}
