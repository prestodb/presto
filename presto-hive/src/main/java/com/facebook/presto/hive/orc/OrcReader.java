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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.orc.metadata.CompressionKind;
import com.facebook.presto.hive.orc.metadata.Footer;
import com.facebook.presto.hive.orc.metadata.Metadata;
import com.facebook.presto.hive.orc.metadata.MetadataReader;
import com.facebook.presto.hive.orc.metadata.PostScript;
import com.facebook.presto.hive.orc.metadata.Type;
import com.facebook.presto.hive.orc.stream.OrcInputStream;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;

public class OrcReader
{
    private static final Logger log = Logger.get(OrcReader.class);

    private static final String MAGIC = "ORC";
    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;
    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;

    private final OrcDataSource orcDataSource;
    private final MetadataReader metadataReader;
    private final TypeManager typeManager;
    private final CompressionKind compressionKind;
    private final int bufferSize;
    private final Footer footer;
    private final Metadata metadata;

    public OrcReader(OrcDataSource orcDataSource, MetadataReader metadataReader, TypeManager typeManager)
            throws IOException
    {
        this.orcDataSource = checkNotNull(orcDataSource, "orcDataSource is null");
        this.metadataReader = checkNotNull(metadataReader, "metadataReader is null");
        this.typeManager = checkNotNull(typeManager, "typeManager is null");

        //
        // Read the file tail:
        //
        // variable: Footer
        // variable: Metadata
        // variable: PostScript - contains length of footer and metadata
        // 3 bytes: file magic "ORC"
        // 1 byte: postScriptSize = PostScript + Magic

        // figure out the size of the file using the option or filesystem
        long size = orcDataSource.getSize();

        // Read the tail of the file
        byte[] buffer = new byte[(int) Math.min(size, EXPECTED_FOOTER_SIZE)];
        orcDataSource.readFully(size - buffer.length, buffer);

        // get length of PostScript - last byte of the file
        int postScriptSize = buffer[buffer.length - SIZE_OF_BYTE] & 0xff;

        // make sure this is an orc file and not an RCFile or something else
        verifyOrcFooter(orcDataSource, postScriptSize, buffer);

        // decode the post script
        int postScriptOffset = buffer.length - SIZE_OF_BYTE - postScriptSize;
        PostScript postScript = metadataReader.readPostScript(buffer, postScriptOffset, postScriptSize);

        // verify this is a supported version
        checkOrcVersion(orcDataSource, postScript.getVersion());

        // check compression codec is supported
        this.compressionKind = postScript.getCompression();

        this.bufferSize = Ints.checkedCast(postScript.getCompressionBlockSize());

        int footerSize = Ints.checkedCast(postScript.getFooterLength());
        int metadataSize = Ints.checkedCast(postScript.getMetadataLength());

        // check if extra bytes need to be read
        Slice completeFooterSlice;
        int completeFooterSize = footerSize + metadataSize + postScriptSize + SIZE_OF_BYTE;
        if (completeFooterSize > buffer.length) {
            // allocate a new buffer large enough for the complete footer
            byte[] newBuffer = new byte[completeFooterSize];
            completeFooterSlice = Slices.wrappedBuffer(newBuffer);

            // initial read was not large enough, so read missing section
            orcDataSource.readFully(size - completeFooterSize, newBuffer, 0, completeFooterSize - buffer.length);

            // copy already read bytes into the new buffer
            completeFooterSlice.setBytes(completeFooterSize - buffer.length, buffer);
        }
        else {
            // footer is already in the bytes in buffer, just adjust position, length
            completeFooterSlice = Slices.wrappedBuffer(buffer, buffer.length - completeFooterSize, completeFooterSize);
        }

        // read metadata
        Slice metadataSlice = completeFooterSlice.slice(0, metadataSize);
        InputStream metadataInputStream = new OrcInputStream(metadataSlice.getInput(), compressionKind, bufferSize);
        this.metadata = metadataReader.readMetadata(metadataInputStream);

        // read footer
        Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);
        InputStream footerInputStream = new OrcInputStream(footerSlice.getInput(), compressionKind, bufferSize);
        this.footer = metadataReader.readFooter(footerInputStream);
    }

    public List<Type> getTypes()
    {
        return footer.getTypes();
    }

    public Footer getFooter()
    {
        return footer;
    }

    public Metadata getMetadata()
    {
        return metadata;
    }

    public CompressionKind getCompressionKind()
    {
        return compressionKind;
    }

    public int getBufferSize()
    {
        return bufferSize;
    }

    public OrcRecordReader createRecordReader(
            long offset,
            long length,
            List<HiveColumnHandle> columnHandles,
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone hiveStorageTimeZone,
            DateTimeZone sessionTimeZone)
            throws IOException
    {
        return new OrcRecordReader(
                footer.getNumberOfRows(),
                footer.getStripes(),
                footer.getFileStats(),
                metadata.getStripeStatsList(),
                orcDataSource,
                offset,
                length,
                checkNotNull(tupleDomain, "tupleDomain is null"),
                checkNotNull(columnHandles, "columnHandles is null"),
                footer.getTypes(),
                compressionKind,
                bufferSize,
                footer.getRowIndexStride(),
                hiveStorageTimeZone,
                sessionTimeZone,
                metadataReader,
                typeManager);
    }

    /**
     * Verify this is an ORC file to prevent users from trying to read text
     * files or RC files as ORC files.
     */
    private static void verifyOrcFooter(
            OrcDataSource fileInput,
            int postScriptSize,
            byte[] buffer)
            throws IOException
    {
        int magicLength = MAGIC.length();
        if (postScriptSize < magicLength + 1) {
            throw new IOException("Malformed ORC file " + fileInput + ". Invalid postscript length " + postScriptSize);
        }

        String magic = new String(buffer, buffer.length - 1 - magicLength, magicLength, UTF_8);
        if (!magic.equals(MAGIC)) {
            // Old versions of ORC (0.11) wrote the magic to the head of the file
            byte[] header = new byte[magicLength];
            fileInput.readFully(0, header);
            magic = new String(header, UTF_8);

            // if it isn't there, this isn't an ORC file
            if (!magic.equals(MAGIC)) {
                throw new IOException("Malformed ORC file " + fileInput + ". Invalid postscript.");
            }
        }
    }

    /**
     * Check to see if this ORC file is from a future version and if so,
     * warn the user that we may not be able to read all of the column encodings.
     */
    private static void checkOrcVersion(OrcDataSource orcDataSource, List<Integer> version)
    {
        if (version.size() >= 1) {
            int major = version.get(0);
            int minor = 0;
            if (version.size() > 1) {
                minor = version.get(1);
            }

            if (major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file " + orcDataSource + " was written by a future Hive version " + Joiner.on('.').join(version) + ". " +
                        "This file may not be readable by this version of Hive.");
            }
        }
    }
}
