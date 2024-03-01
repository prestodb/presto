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
// Copyright 2015 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.facebook.presto.druid.zip;

import com.facebook.presto.druid.DataInputSource;
import com.facebook.presto.spi.PrestoException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipException;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A representation of a ZIP file. Contains the file comment, encoding, and entries. Also contains
 * internal information about the structure and location of ZIP file parts.
 */
public class ZipFileData
{
    private final Charset charset;
    private final Map<String, ZipFileEntry> entries;

    private String comment;
    private long centralDirectorySize;
    private long centralDirectoryOffset;
    private long expectedEntries;
    private long numEntries;
    private boolean maybeZip64;
    private boolean isZip64;
    private long zip64EndOfCentralDirectoryOffset;

    public ZipFileData(Charset charset)
    {
        checkArgument(charset != null, "Zip file charset could not be null");
        this.charset = charset;
        comment = "";
        entries = new LinkedHashMap<>();
    }

    public Charset getCharset()
    {
        return charset;
    }

    public String getComment()
    {
        return comment;
    }

    public void setComment(byte[] comment)
    {
        checkArgument(comment != null, "Zip file comment could not be null");
        if (comment.length > 0xffff) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("File comment too long. Is %d; max %d.", comment.length, 0xffff));
        }
        this.comment = fromBytes(comment);
    }

    public void setComment(String comment)
    {
        setComment(getBytes(comment));
    }

    public long getCentralDirectorySize()
    {
        return centralDirectorySize;
    }

    public void setCentralDirectorySize(long centralDirectorySize)
    {
        this.centralDirectorySize = centralDirectorySize;
        if (centralDirectorySize > 0xffffffffL) {
            setZip64(true);
        }
    }

    public long getCentralDirectoryOffset()
    {
        return centralDirectoryOffset;
    }

    public void setCentralDirectoryOffset(long offset)
    {
        this.centralDirectoryOffset = offset;
        if (centralDirectoryOffset > 0xffffffffL) {
            setZip64(true);
        }
    }

    public long getExpectedEntries()
    {
        return expectedEntries;
    }

    public void setExpectedEntries(long count)
    {
        this.expectedEntries = count;
        if (expectedEntries > 0xffff) {
            setZip64(true);
        }
    }

    public long getNumEntries()
    {
        return numEntries;
    }

    private void setNumEntries(long numEntries)
    {
        this.numEntries = numEntries;
        if (numEntries > 0xffff) {
            setZip64(true);
        }
    }

    public Collection<ZipFileEntry> getEntries()
    {
        return entries.values();
    }

    public ZipFileEntry getEntry(@Nullable String name)
    {
        return entries.get(name);
    }

    public void addEntry(ZipFileEntry entry)
    {
        entries.put(entry.getName(), entry);
        setNumEntries(numEntries + 1);
        if (entry.getFeatureSet().contains(ZipFileEntry.Feature.ZIP64_SIZE)
                || entry.getFeatureSet().contains(ZipFileEntry.Feature.ZIP64_CSIZE)
                || entry.getFeatureSet().contains(ZipFileEntry.Feature.ZIP64_OFFSET)) {
            setZip64(true);
        }
    }

    public boolean isMaybeZip64()
    {
        return maybeZip64;
    }

    public void setMaybeZip64(boolean maybeZip64)
    {
        this.maybeZip64 = maybeZip64;
    }

    public boolean isZip64()
    {
        return isZip64;
    }

    public void setZip64(boolean isZip64)
    {
        this.isZip64 = isZip64;
        setMaybeZip64(true);
    }

    public long getZip64EndOfCentralDirectoryOffset()
    {
        return zip64EndOfCentralDirectoryOffset;
    }

    public void setZip64EndOfCentralDirectoryOffset(long offset)
    {
        this.zip64EndOfCentralDirectoryOffset = offset;
        setZip64(true);
    }

    public byte[] getBytes(String string)
    {
        return string.getBytes(charset);
    }

    public String fromBytes(byte[] bytes)
    {
        return new String(bytes, charset);
    }

    /**
     * Finds, reads and parses ZIP file entries from the central directory.
     */
    public ZipFileData createZipFileData(DataInputSource dataInputSource)
            throws IOException
    {
        long eocdLocation = findEndOfCentralDirectoryRecord(dataInputSource);
        ZipFileData fileData = new ZipFileData(UTF_8);
        EndOfCentralDirectoryRecord.read(fileData, dataInputSource, eocdLocation);

        if (fileData.isMaybeZip64()) {
            try {
                Zip64EndOfCentralDirectoryLocator.read(fileData, dataInputSource, eocdLocation - Zip64EndOfCentralDirectoryLocator.FIXED_DATA_SIZE);
                Zip64EndOfCentralDirectory.read(fileData, dataInputSource, fileData.getZip64EndOfCentralDirectoryOffset());
            }
            catch (ZipException e) {
                // expected if not in Zip64 format
            }
        }

        if (fileData.isZip64()) {
            // If in Zip64 format or using strict entry numbers, use the parsed information as is to read
            // the central directory file headers.
            readCentralDirectoryFileHeaders(fileData, dataInputSource, fileData.getCentralDirectoryOffset(), fileData.getCharset(), fileData.getExpectedEntries());
        }
        else {
            // If not in Zip64 format, compute central directory offset by end of central directory record
            // offset and central directory size to allow reading large non-compliant Zip32 directories.
            long centralDirectoryOffset = eocdLocation - fileData.getCentralDirectorySize();
            // If the lower 4 bytes match, the above calculation is correct; otherwise fallback to
            // reported offset.
            if ((int) centralDirectoryOffset == (int) fileData.getCentralDirectoryOffset()) {
                readCentralDirectoryFileHeaders(fileData, dataInputSource, centralDirectoryOffset, fileData.getCharset());
            }
            else {
                readCentralDirectoryFileHeaders(fileData, dataInputSource, fileData.getCentralDirectoryOffset(), fileData.getCharset(), fileData.getExpectedEntries());
            }
        }
        return fileData;
    }

    /**
     * Finds the file offset of the end of central directory record.
     */
    private long findEndOfCentralDirectoryRecord(DataInputSource dataInputSource)
            throws IOException
    {
        long fileSize = dataInputSource.getSize();
        byte[] signature = ZipUtil.intToLittleEndian(EndOfCentralDirectoryRecord.SIGNATURE);
        byte[] buffer = new byte[(int) Math.min(64, fileSize)];
        int readLength = buffer.length;
        if (readLength < EndOfCentralDirectoryRecord.FIXED_DATA_SIZE) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Zip file '%s' is malformed. It does not contain an end of central directory record.", dataInputSource.getId()));
        }

        long offset = fileSize - buffer.length;
        while (offset >= 0) {
            dataInputSource.readFully(offset, buffer, 0, readLength);
            int signatureLocation = scanBackwards(signature, buffer, buffer.length);
            while (signatureLocation != -1) {
                long eocdSize = fileSize - offset - signatureLocation;
                if (eocdSize >= EndOfCentralDirectoryRecord.FIXED_DATA_SIZE) {
                    int commentLength = ZipUtil.getUnsignedShort(
                            buffer,
                            signatureLocation + EndOfCentralDirectoryRecord.COMMENT_LENGTH_OFFSET);
                    long readCommentLength = eocdSize - EndOfCentralDirectoryRecord.FIXED_DATA_SIZE;
                    if (commentLength == readCommentLength) {
                        return offset + signatureLocation;
                    }
                }
                signatureLocation = scanBackwards(signature, buffer, signatureLocation - 1);
            }
            readLength = buffer.length - 3;
            buffer[buffer.length - 3] = buffer[0];
            buffer[buffer.length - 2] = buffer[1];
            buffer[buffer.length - 1] = buffer[2];
            offset -= readLength;
        }
        throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Zip file '%s' is malformed. It does not contain an end of central directory record.", dataInputSource.getId()));
    }

    /**
     * Reads and parses ZIP file entries from the central directory.
     */
    private void readCentralDirectoryFileHeaders(ZipFileData fileData, DataInputSource dataInputSource, long fileOffset, Charset charset, long count)
            throws IOException
    {
        try {
            long position = fileOffset;
            for (long i = 0; i < count; i++) {
                position += CentralDirectoryFileHeader.read(fileData, dataInputSource, position, charset);
            }
        }
        catch (ZipException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
    }

    /**
     * Reads and parses ZIP file entries from the central directory.
     */
    private void readCentralDirectoryFileHeaders(ZipFileData fileData, DataInputSource dataInputSource, long fileOffset, Charset charset)
            throws IOException
    {
        try {
            long position = fileOffset;
            while ((position - fileOffset) < fileData.getCentralDirectorySize()) {
                position += CentralDirectoryFileHeader.read(fileData, dataInputSource, position, charset);
            }
        }
        catch (ZipException e) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, e);
        }
    }

    /**
     * Looks for the target sub array in the buffer scanning backwards starting at offset. Returns the
     * index where the target is found or -1 if not found.
     */
    private int scanBackwards(byte[] target, byte[] buffer, int offset)
    {
        int start = Math.min(offset, buffer.length - target.length);
        for (int i = start; i >= 0; i--) {
            for (int j = 0; j < target.length; j++) {
                if (buffer[i + j] != target[j]) {
                    break;
                }
                else if (j == target.length - 1) {
                    return i;
                }
            }
        }
        return -1;
    }
}
