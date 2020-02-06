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

import static com.facebook.presto.druid.DruidErrorCode.DRUID_SEGMENT_LOAD_ERROR;
import static java.lang.String.format;

public class EndOfCentralDirectoryRecord
{
    public static final int SIGNATURE = 0x06054b50;
    public static final int FIXED_DATA_SIZE = 22;
    public static final int DISK_NUMBER_OFFSET = 4;
    public static final int CD_DISK_OFFSET = 6;
    public static final int DISK_ENTRIES_OFFSET = 8;
    public static final int TOTAL_ENTRIES_OFFSET = 10;
    public static final int CD_SIZE_OFFSET = 12;
    public static final int CD_OFFSET_OFFSET = 16;
    public static final int COMMENT_LENGTH_OFFSET = 20;

    private EndOfCentralDirectoryRecord()
    {
    }

    /**
     * Read the end of central directory record from the input stream and parse {@link ZipFileData}
     * from it.
     */
    public static void read(ZipFileData zipFileData, DataInputSource dataInputSource, long offset)
            throws IOException
    {
        long position = offset;
        byte[] fixedSizeData = new byte[FIXED_DATA_SIZE];

        dataInputSource.readFully(position, fixedSizeData, 0, FIXED_DATA_SIZE);
        position += FIXED_DATA_SIZE;
        if (!ZipUtil.arrayStartsWith(fixedSizeData, ZipUtil.intToLittleEndian(SIGNATURE))) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Malformed End of Central Directory Record; does not start with %08x", SIGNATURE));
        }

        byte[] comment = new byte[ZipUtil.getUnsignedShort(fixedSizeData, COMMENT_LENGTH_OFFSET)];
        if (comment.length > 0) {
            dataInputSource.readFully(position, comment, 0, comment.length);
        }
        short diskNumber = ZipUtil.get16(fixedSizeData, DISK_NUMBER_OFFSET);
        short centralDirectoryDisk = ZipUtil.get16(fixedSizeData, CD_DISK_OFFSET);
        short entriesOnDisk = ZipUtil.get16(fixedSizeData, DISK_ENTRIES_OFFSET);
        short totalEntries = ZipUtil.get16(fixedSizeData, TOTAL_ENTRIES_OFFSET);
        int centralDirectorySize = ZipUtil.get32(fixedSizeData, CD_SIZE_OFFSET);
        int centralDirectoryOffset = ZipUtil.get32(fixedSizeData, CD_OFFSET_OFFSET);
        if (diskNumber == -1 || centralDirectoryDisk == -1 || entriesOnDisk == -1
                || totalEntries == -1 || centralDirectorySize == -1 || centralDirectoryOffset == -1) {
            zipFileData.setMaybeZip64(true);
        }
        zipFileData.setComment(comment);
        zipFileData.setCentralDirectorySize(ZipUtil.getUnsignedInt(fixedSizeData, CD_SIZE_OFFSET));
        zipFileData.setCentralDirectoryOffset(ZipUtil.getUnsignedInt(fixedSizeData, CD_OFFSET_OFFSET));
        zipFileData.setExpectedEntries(ZipUtil.getUnsignedShort(fixedSizeData, TOTAL_ENTRIES_OFFSET));
    }
}
