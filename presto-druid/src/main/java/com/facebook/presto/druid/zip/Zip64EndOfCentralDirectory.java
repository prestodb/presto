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
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public class Zip64EndOfCentralDirectory
{
    static final int SIGNATURE = 0x06064b50;
    static final int FIXED_DATA_SIZE = 56;
    static final int TOTAL_ENTRIES_OFFSET = 32;
    static final int CD_OFFSET_OFFSET = 48;

    private Zip64EndOfCentralDirectory()
    {
    }

    /**
     * Read the Zip64 end of central directory record from the input stream and parse additional
     * {@link ZipFileData} from it.
     */
    public static void read(ZipFileData file, DataInputSource dataInputSource, long offset)
            throws IOException
    {
        checkArgument(file != null, "Zip file data for source:%s is null", dataInputSource.getId());

        byte[] fixedSizeData = new byte[FIXED_DATA_SIZE];
        dataInputSource.readFully(offset, fixedSizeData);
        if (!ZipUtil.arrayStartsWith(fixedSizeData, ZipUtil.intToLittleEndian(SIGNATURE))) {
            throw new PrestoException(DRUID_SEGMENT_LOAD_ERROR, format("Malformed End of Central Directory Record; does not start with %08x", SIGNATURE));
        }
        file.setZip64(true);
        file.setCentralDirectoryOffset(ZipUtil.getUnsignedLong(fixedSizeData, CD_OFFSET_OFFSET));
        file.setExpectedEntries(ZipUtil.getUnsignedLong(fixedSizeData, TOTAL_ENTRIES_OFFSET));
    }
}
