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
package com.facebook.presto.druid.zip;

import com.facebook.presto.druid.DataInputSource;
import com.facebook.presto.spi.PrestoException;

import java.io.IOException;
import java.nio.charset.Charset;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;

public class CentralDirectoryFileHeader
{
    static final int SIGNATURE = 0x02014b50;
    static final int FIXED_DATA_SIZE = 46;
    static final int VERSION_OFFSET = 4;
    static final int VERSION_NEEDED_OFFSET = 6;
    static final int FLAGS_OFFSET = 8;
    static final int METHOD_OFFSET = 10;
    static final int MOD_TIME_OFFSET = 12;
    static final int CRC_OFFSET = 16;
    static final int COMPRESSED_SIZE_OFFSET = 20;
    static final int UNCOMPRESSED_SIZE_OFFSET = 24;
    static final int FILENAME_LENGTH_OFFSET = 28;
    static final int EXTRA_FIELD_LENGTH_OFFSET = 30;
    static final int COMMENT_LENGTH_OFFSET = 32;
    static final int INTERNAL_ATTRIBUTES_OFFSET = 36;
    static final int EXTERNAL_ATTRIBUTES_OFFSET = 38;
    static final int LOCAL_HEADER_OFFSET_OFFSET = 42;

    private CentralDirectoryFileHeader()
    {
    }

    // read one ZipFileEntry and add into ZipFileData
    public static long read(ZipFileData fileData, DataInputSource dataInputSource, long fileOffset, Charset charset)
            throws IOException
    {
        long position = fileOffset;

        byte[] fixedSizeData = new byte[FIXED_DATA_SIZE];
        dataInputSource.readFully(position, fixedSizeData);
        position += fixedSizeData.length;
        if (!ZipUtil.arrayStartsWith(fixedSizeData, ZipUtil.intToLittleEndian(SIGNATURE))) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, String.format("Malformed Central Directory File Header; does not start with %08x", SIGNATURE));
        }

        byte[] name = new byte[ZipUtil.getUnsignedShort(fixedSizeData, FILENAME_LENGTH_OFFSET)];
        byte[] extraField = new byte[ZipUtil.getUnsignedShort(fixedSizeData, EXTRA_FIELD_LENGTH_OFFSET)];
        byte[] comment = new byte[ZipUtil.getUnsignedShort(fixedSizeData, COMMENT_LENGTH_OFFSET)];

        if (name.length > 0) {
            dataInputSource.readFully(position, name);
            position += name.length;
        }
        if (extraField.length > 0) {
            dataInputSource.readFully(position, extraField);
            position += extraField.length;
        }
        if (comment.length > 0) {
            dataInputSource.readFully(position, comment);
            position += extraField.length;
        }

        ExtraDataList extra = new ExtraDataList(extraField);

        long csize = ZipUtil.getUnsignedInt(fixedSizeData, COMPRESSED_SIZE_OFFSET);
        long size = ZipUtil.getUnsignedInt(fixedSizeData, UNCOMPRESSED_SIZE_OFFSET);
        long offset = ZipUtil.getUnsignedInt(fixedSizeData, LOCAL_HEADER_OFFSET_OFFSET);
        if (csize == 0xffffffffL || size == 0xffffffffL || offset == 0xffffffffL) {
            ExtraData zip64Extra = extra.get((short) 0x0001);
            if (zip64Extra != null) {
                int index = 0;
                if (size == 0xffffffffL) {
                    size = ZipUtil.getUnsignedLong(zip64Extra.getData(), index);
                    index += 8;
                }
                if (csize == 0xffffffffL) {
                    csize = ZipUtil.getUnsignedLong(zip64Extra.getData(), index);
                    index += 8;
                }
                if (offset == 0xffffffffL) {
                    offset = ZipUtil.getUnsignedLong(zip64Extra.getData(), index);
                    index += 8;
                }
            }
        }

        ZipFileEntry entry = new ZipFileEntry(new String(name, charset));
        entry.setVersion(ZipUtil.get16(fixedSizeData, VERSION_OFFSET));
        entry.setVersionNeeded(ZipUtil.get16(fixedSizeData, VERSION_NEEDED_OFFSET));
        entry.setFlags(ZipUtil.get16(fixedSizeData, FLAGS_OFFSET));
        entry.setMethod(ZipFileEntry.Compression.fromValue(ZipUtil.get16(fixedSizeData, METHOD_OFFSET)));
        long time = ZipUtil.dosToUnixTime(ZipUtil.get32(fixedSizeData, MOD_TIME_OFFSET));
        entry.setTime(ZipUtil.isValidInDos(time) ? time : ZipUtil.DOS_EPOCH);
        entry.setCrc(ZipUtil.getUnsignedInt(fixedSizeData, CRC_OFFSET));
        entry.setCompressedSize(csize);
        entry.setSize(size);
        entry.setInternalAttributes(ZipUtil.get16(fixedSizeData, INTERNAL_ATTRIBUTES_OFFSET));
        entry.setExternalAttributes(ZipUtil.get32(fixedSizeData, EXTERNAL_ATTRIBUTES_OFFSET));
        entry.setLocalHeaderOffset(offset);
        entry.setExtra(extra);
        entry.setComment(new String(comment, charset));

        fileData.addEntry(entry);
        return position - fileOffset;
    }
}
