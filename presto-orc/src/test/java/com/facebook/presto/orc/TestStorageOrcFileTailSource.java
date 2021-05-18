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

import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.orc.metadata.DwrfMetadataReader;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.proto.DwrfProto;
import com.facebook.presto.orc.protobuf.AbstractMessageLite;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestStorageOrcFileTailSource
{
    private static final DataSize DEFAULT_SIZE = new DataSize(1, DataSize.Unit.MEGABYTE);

    private TempFile file;
    private MetadataReader metadataReader;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        this.file = new TempFile();
        this.metadataReader = new DwrfMetadataReader();
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
        // beef up the file size to make sure it's larger than the expectedFooterSize = 567 we will use below
        FileOutputStream out = new FileOutputStream(file.getFile());
        out.write(new byte[100 * 1000]);

        // write the post script
        DwrfProto.PostScript.Builder postScript = DwrfProto.PostScript.newBuilder()
                .setFooterLength(0)
                .setCompression(DwrfProto.CompressionKind.NONE);
        writeTail(postScript, out);
        out.close();

        // read the OrcFileTail
        int expectedFooterSize = 567;
        StorageOrcFileTailSource src = new StorageOrcFileTailSource(expectedFooterSize);
        TestingOrcDataSource orcDataSource = new TestingOrcDataSource(createFileOrcDataSource());
        src.getOrcFileTail(orcDataSource, metadataReader, Optional.empty(), false);

        // make sure only the configured expectedFooterSize bytes have been read
        assertEquals(orcDataSource.getReadCount(), 1);
        DiskRange lastReadRange = orcDataSource.getLastReadRanges().get(0);
        assertEquals(lastReadRange.getLength(), expectedFooterSize);
    }

    private OrcDataSource createFileOrcDataSource()
            throws FileNotFoundException
    {
        return new FileOrcDataSource(file.getFile(), DEFAULT_SIZE, DEFAULT_SIZE, DEFAULT_SIZE, false);
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
}
