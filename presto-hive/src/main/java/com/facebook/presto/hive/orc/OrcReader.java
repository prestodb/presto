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
import com.facebook.presto.hive.orc.stream.OrcInputStream;
import com.facebook.presto.hive.shaded.com.google.protobuf.CodedInputStream;
import com.facebook.presto.spi.TupleDomain;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Footer;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Metadata;
import org.apache.hadoop.hive.ql.io.orc.OrcProto.Type;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.NONE;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.SNAPPY;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.CompressionKind.ZLIB;
import static org.apache.hadoop.hive.ql.io.orc.OrcProto.PostScript;

public class OrcReader
{
    private static final Logger log = Logger.get(OrcReader.class);

    private static final String MAGIC = "ORC";
    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;
    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;

    private final Path path;
    private final FileSystem fileSystem;
    private final CompressionKind compressionKind;
    private final int bufferSize;
    private final Footer footer;
    private final Metadata metadata;

    public OrcReader(Path path, FileSystem fileSystem)
            throws IOException
    {
        this.path = checkNotNull(path, "path is null");
        this.fileSystem = checkNotNull(fileSystem, "fileSystem is null");

        //
        // Read the file tail:
        //
        // variable: Footer
        // variable: Metadata
        // variable: PostScript - contains length of footer and metadata
        // 3 bytes: file magic "ORC"
        // 1 byte: postScriptSize = PostScript + Magic

        try (FSDataInputStream file = fileSystem.open(path)) {
            // figure out the size of the file using the option or filesystem
            long size = fileSystem.getFileStatus(path).getLen();

            // Read the tail of the file
            byte[] buffer = new byte[(int) Math.min(size, EXPECTED_FOOTER_SIZE)];
            file.readFully(size - buffer.length, buffer);

            // get length of PostScript - last byte of the file
            int postScriptSize = buffer[buffer.length - SIZE_OF_BYTE] & 0xff;

            // make sure this is an orc file and not an RCFile or something else
            verifyOrcFooter(file, path, postScriptSize, buffer);

            // decode the post script
            int postScriptOffset = buffer.length - SIZE_OF_BYTE - postScriptSize;
            CodedInputStream in = CodedInputStream.newInstance(buffer, postScriptOffset, postScriptSize);
            PostScript postScript = PostScript.parseFrom(in);

            // verify this is a supported version
            checkOrcVersion(path, postScript.getVersionList());

            // check compression codec is supported
            this.compressionKind = postScript.getCompression();
            checkArgument(compressionKind == NONE || compressionKind == SNAPPY || compressionKind == ZLIB, "%s compression not implemented yet", compressionKind);

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
                file.readFully(size - completeFooterSize, newBuffer, 0, completeFooterSize - buffer.length);

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
            this.metadata = Metadata.parseFrom(metadataInputStream);

            // read footer
            Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);
            InputStream footerInputStream = new OrcInputStream(footerSlice.getInput(), compressionKind, bufferSize);
            this.footer = Footer.parseFrom(footerInputStream);
        }
    }

    public List<Type> getTypes()
    {
        return footer.getTypesList();
    }

    public OrcRecordReader createRecordReader(
            long offset,
            long length,
            List<HiveColumnHandle> columnHandles,
            TupleDomain<HiveColumnHandle> tupleDomain,
            DateTimeZone sessionTimeZone)
            throws IOException
    {
        return new OrcRecordReader(
                footer.getStripesList(),
                metadata.getStripeStatsList(),
                fileSystem,
                path,
                offset,
                length,
                checkNotNull(tupleDomain, "tupleDomain is null"),
                checkNotNull(columnHandles, "columnHandles is null"),
                footer.getTypesList(),
                compressionKind,
                bufferSize,
                footer.getRowIndexStride(),
                sessionTimeZone);
    }

    /**
     * Verify this is an ORC file to prevent users from trying to read text
     * files or RC files as ORC files.
     */
    private static void verifyOrcFooter(
            FSDataInputStream in,
            Path path,
            int postScriptSize,
            byte[] buffer)
            throws IOException
    {
        int magicLength = MAGIC.length();
        if (postScriptSize < magicLength + 1) {
            throw new IOException("Malformed ORC file " + path + ". Invalid postscript length " + postScriptSize);
        }

        String magic = new String(buffer, buffer.length - 1 - magicLength, magicLength, UTF_8);
        if (!magic.equals(MAGIC)) {
            // Old versions of ORC (0.11) wrote the magic to the head of the file
            byte[] header = new byte[magicLength];
            in.readFully(0, header);
            magic = new String(header, UTF_8);

            // if it isn't there, this isn't an ORC file
            if (!magic.equals(MAGIC)) {
                throw new IOException("Malformed ORC file " + path + ". Invalid postscript.");
            }
        }
    }

    /**
     * Check to see if this ORC file is from a future version and if so,
     * warn the user that we may not be able to read all of the column encodings.
     */
    private static void checkOrcVersion(Path path, List<Integer> version)
    {
        if (version.size() >= 1) {
            int major = version.get(0);
            int minor = 0;
            if (version.size() > 1) {
                minor = version.get(1);
            }

            if (major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file " + path + " was written by a future Hive version " + Joiner.on('.').join(version) + ". " +
                        "This file may not be readable by this version of Hive.");
            }
        }
    }
}
