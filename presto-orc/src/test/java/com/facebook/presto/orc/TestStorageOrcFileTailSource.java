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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.DwrfStripeCacheData;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.AbstractMessageLite;
import com.facebook.presto.orc.protobuf.InvalidProtocolBufferException;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import static com.facebook.presto.orc.metadata.DwrfStripeCacheMode.INDEX_AND_FOOTER;
import static com.facebook.presto.orc.proto.DwrfProto.CompressionKind.NONE;
import static com.facebook.presto.orc.proto.DwrfProto.StripeCacheMode.BOTH;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestStorageOrcFileTailSource
{
    private static final DataSize DEFAULT_SIZE = new DataSize(1, MEGABYTE);
    private static final int FOOTER_READ_SIZE_IN_BYTES = (int) DEFAULT_SIZE.toBytes();

    private TempFile file;
    private MetadataReader metadataReader;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        this.file = new TempFile();
        this.metadataReader = new DwrfMetadataReader(new RuntimeStats());
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        file.close();
    }

    @Test
    public void testReadExpectedFooterSize()
            throws IOException
    {
        // beef up the file size to make sure it's larger than the expectedFooterSizeInBytes = 567 we will use below
        FileOutputStream out = new FileOutputStream(file.getFile());
        out.write(new byte[100 * 1000]);

        // write the post script
        DwrfProto.PostScript.Builder postScript = DwrfProto.PostScript.newBuilder()
                .setFooterLength(0)
                .setCompression(NONE);
        writeTail(postScript, out);
        out.close();

        // read the OrcFileTail
        int expectedFooterSizeInBytes = 567;
        StorageOrcFileTailSource src = new StorageOrcFileTailSource(expectedFooterSizeInBytes, false);
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(createFileOrcDataSource());
        src.getOrcFileTail(orcDataSource, metadataReader, Optional.empty(), false);

        // make sure only the configured expectedFooterSizeInBytes bytes have been read
        assertEquals(orcDataSource.getReadCount(), 1);
        DiskRange lastReadRange = orcDataSource.getLastReadRanges().get(0);
        assertEquals(lastReadRange.getLength(), expectedFooterSizeInBytes);
    }

    @Test
    public void testSkipDwrfStripeCacheIfDisabled()
            throws IOException
    {
        // beef up the file size to make sure the file can fit the 100 byte long stripe cache
        FileOutputStream out = new FileOutputStream(file.getFile());
        out.write(new byte[100 * 1000]);

        // write the footer and post script
        DwrfProto.Footer.Builder footer = DwrfProto.Footer.newBuilder()
                .addAllStripeCacheOffsets(ImmutableList.of(0, 256, 512));
        DwrfProto.PostScript.Builder postScript = DwrfProto.PostScript.newBuilder()
                .setCompression(NONE)
                .setCacheMode(BOTH)
                .setCacheSize(512);
        writeTail(footer, postScript, out);
        out.close();

        int tailReadSizeInBytes = 256;
        // read the file tail with the disabled "read dwrf stripe cache" feature
        StorageOrcFileTailSource src = new StorageOrcFileTailSource(tailReadSizeInBytes, false);
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(createFileOrcDataSource());
        OrcFileTail orcFileTail = src.getOrcFileTail(orcDataSource, metadataReader, Optional.empty(), false);

        assertEquals(orcFileTail.getMetadataSize(), 0);
        DwrfProto.Footer actualFooter = readFooter(orcFileTail);
        assertEquals(actualFooter, footer.build());

        // make sure the stripe cache has not been read
        assertFalse(orcFileTail.getDwrfStripeCacheData().isPresent());
        assertEquals(orcDataSource.getReadCount(), 1);
        DiskRange lastReadRange = orcDataSource.getLastReadRanges().get(0);
        assertEquals(lastReadRange.getLength(), tailReadSizeInBytes);
    }

    @Test
    public void testReadDwrfStripeCacheIfEnabled()
            throws IOException
    {
        FileOutputStream out = new FileOutputStream(file.getFile());

        // write a fake stripe cache
        byte[] stripeCache = new byte[100];
        for (int i = 0; i < stripeCache.length; i++) {
            stripeCache[i] = (byte) i;
        }
        out.write(stripeCache);

        // write the footer and post script
        DwrfProto.Footer.Builder footer = DwrfProto.Footer.newBuilder()
                .addAllStripeCacheOffsets(ImmutableList.of(1, 2, 3));
        DwrfProto.PostScript.Builder postScript = DwrfProto.PostScript.newBuilder()
                .setCompression(NONE)
                .setCacheMode(BOTH)
                .setCacheSize(stripeCache.length);
        writeTail(footer, postScript, out);
        out.close();

        // read the file tail with the enabled "read dwrf stripe cache" feature
        StorageOrcFileTailSource src = new StorageOrcFileTailSource(FOOTER_READ_SIZE_IN_BYTES, true);
        OrcDataSource orcDataSource = createFileOrcDataSource();
        OrcFileTail orcFileTail = src.getOrcFileTail(orcDataSource, metadataReader, Optional.empty(), false);

        assertEquals(orcFileTail.getMetadataSize(), 0);
        DwrfProto.Footer actualFooter = readFooter(orcFileTail);
        assertEquals(actualFooter, footer.build());

        // make sure the stripe cache is loaded correctly
        assertTrue(orcFileTail.getDwrfStripeCacheData().isPresent());
        DwrfStripeCacheData dwrfStripeCacheData = orcFileTail.getDwrfStripeCacheData().get();
        assertEquals(dwrfStripeCacheData.getDwrfStripeCacheMode(), INDEX_AND_FOOTER);
        assertEquals(dwrfStripeCacheData.getDwrfStripeCacheSize(), stripeCache.length);
        assertEquals(dwrfStripeCacheData.getDwrfStripeCacheSlice().getBytes(), stripeCache);
    }

    @Test
    public void testReadDwrfStripeCacheIfEnabledButAbsent()
            throws IOException
    {
        FileOutputStream out = new FileOutputStream(file.getFile());

        // write the footer and post script
        DwrfProto.Footer.Builder footer = DwrfProto.Footer.newBuilder();
        DwrfProto.PostScript.Builder postScript = DwrfProto.PostScript.newBuilder()
                .setCompression(NONE);
        writeTail(footer, postScript, out);
        out.close();

        // read the file tail with the enabled "read dwrf stripe cache" feature
        StorageOrcFileTailSource src = new StorageOrcFileTailSource(FOOTER_READ_SIZE_IN_BYTES, true);
        OrcDataSource orcDataSource = createFileOrcDataSource();
        OrcFileTail orcFileTail = src.getOrcFileTail(orcDataSource, metadataReader, Optional.empty(), false);

        assertEquals(orcFileTail.getMetadataSize(), 0);
        DwrfProto.Footer actualFooter = readFooter(orcFileTail);
        assertEquals(actualFooter, footer.build());

        // the feature is enabled, but file doesn't have the stripe cache
        assertFalse(orcFileTail.getDwrfStripeCacheData().isPresent());
    }

    private OrcDataSource createFileOrcDataSource()
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file.getFile(), DEFAULT_SIZE, DEFAULT_SIZE, DEFAULT_SIZE, false);
    }

    /**
     * Write footer + post script, and return the number of bytes written.
     */
    private int writeTail(DwrfProto.Footer.Builder footer, DwrfProto.PostScript.Builder postScript, OutputStream out)
            throws IOException
    {
        int footerSize = writeObject(footer.build(), out);
        postScript.setFooterLength(footerSize);
        int postScriptSize = writeTail(postScript, out);
        return footerSize + postScriptSize;
    }

    /**
     * Write the post script, and return the number of bytes written.
     */
    private int writeTail(DwrfProto.PostScript.Builder postScript, OutputStream out)
            throws IOException
    {
        int postScriptSize = writeObject(postScript.build(), out);
        out.write(postScriptSize & 0xff);
        return postScriptSize + 1;
    }

    private int writeObject(AbstractMessageLite msg, OutputStream out)
            throws IOException
    {
        byte[] bytes = msg.toByteArray();
        out.write(bytes);
        return bytes.length;
    }

    private DwrfProto.Footer readFooter(OrcFileTail orcFileTail)
            throws InvalidProtocolBufferException
    {
        return DwrfProto.Footer.parseFrom(orcFileTail.getFooterSlice().getBytes(0, orcFileTail.getFooterSize()));
    }
}
