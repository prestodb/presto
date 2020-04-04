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
package com.facebook.presto.orc.cache;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcWriteValidation;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.MetadataReader;
import com.facebook.presto.orc.metadata.OrcFileTail;
import com.facebook.presto.orc.metadata.PostScript;
import com.google.common.base.Joiner;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.orc.OrcReader.validateWrite;
import static com.facebook.presto.orc.metadata.PostScript.MAGIC;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;

public class StorageOrcFileTailSource
        implements OrcFileTailSource
{
    private static final Logger log = Logger.get(StorageOrcFileTailSource.class);

    private static final int EXPECTED_FOOTER_SIZE = 16 * 1024;

    private static final int CURRENT_MAJOR_VERSION = 0;
    private static final int CURRENT_MINOR_VERSION = 12;

    @Override
    public OrcFileTail getOrcFileTail(OrcDataSource orcDataSource, MetadataReader metadataReader, Optional<OrcWriteValidation> writeValidation, boolean cacheable)
            throws IOException
    {
        long size = orcDataSource.getSize();
        if (size <= MAGIC.length()) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid file size %s", size);
        }

        // Read the tail of the file
        byte[] buffer = new byte[toIntExact(min(size, EXPECTED_FOOTER_SIZE))];
        orcDataSource.readFully(size - buffer.length, buffer);

        // get length of PostScript - last byte of the file
        int postScriptSize = buffer[buffer.length - SIZE_OF_BYTE] & 0xff;
        if (postScriptSize >= buffer.length) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Invalid postscript length %s", postScriptSize);
        }

        // decode the post script
        PostScript postScript;
        try {
            postScript = metadataReader.readPostScript(buffer, buffer.length - SIZE_OF_BYTE - postScriptSize, postScriptSize);
        }
        catch (OrcCorruptionException e) {
            // check if this is an ORC file and not an RCFile or something else
            if (!isValidHeaderMagic(orcDataSource)) {
                throw new OrcCorruptionException(orcDataSource.getId(), "Not an ORC file");
            }
            throw e;
        }

        // verify this is a supported version
        checkOrcVersion(orcDataSource, postScript.getVersion());
        validateWrite(writeValidation, orcDataSource, validation -> validation.getVersion().equals(postScript.getVersion()), "Unexpected version");

        int bufferSize = toIntExact(postScript.getCompressionBlockSize());

        // check compression codec is supported
        CompressionKind compressionKind = postScript.getCompression();
        validateWrite(writeValidation, orcDataSource, validation -> validation.getCompression() == compressionKind, "Unexpected compression");

        PostScript.HiveWriterVersion hiveWriterVersion = postScript.getHiveWriterVersion();

        int footerSize = toIntExact(postScript.getFooterLength());
        int metadataSize = toIntExact(postScript.getMetadataLength());

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

        Slice metadataSlice = completeFooterSlice.slice(0, metadataSize);
        Slice footerSlice = completeFooterSlice.slice(metadataSize, footerSize);

        return new OrcFileTail(hiveWriterVersion, bufferSize, compressionKind, footerSlice, footerSize, metadataSlice, metadataSize);
    }

    /**
     * Does the file start with the ORC magic bytes?
     */
    private static boolean isValidHeaderMagic(OrcDataSource source)
            throws IOException
    {
        byte[] headerMagic = new byte[MAGIC.length()];
        source.readFully(0, headerMagic);

        return MAGIC.equals(Slices.wrappedBuffer(headerMagic));
    }

    /**
     * Check to see if this ORC file is from a future version and if so,
     * warn the user that we may not be able to read all of the column encodings.
     */
    // This is based on the Apache Hive ORC code
    private static void checkOrcVersion(OrcDataSource orcDataSource, List<Integer> version)
    {
        if (version.size() >= 1) {
            int major = version.get(0);
            int minor = 0;
            if (version.size() > 1) {
                minor = version.get(1);
            }

            if (major > CURRENT_MAJOR_VERSION || (major == CURRENT_MAJOR_VERSION && minor > CURRENT_MINOR_VERSION)) {
                log.warn("ORC file %s was written by a newer Hive version %s. This file may not be readable by this version of Hive (%s.%s).",
                        orcDataSource,
                        Joiner.on('.').join(version),
                        CURRENT_MAJOR_VERSION,
                        CURRENT_MINOR_VERSION);
            }
        }
    }
}
