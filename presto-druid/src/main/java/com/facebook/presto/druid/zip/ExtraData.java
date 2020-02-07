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

import java.util.Arrays;

/**
 * A holder class for extra data in a ZIP entry.
 */
final class ExtraData
{
    static final int ID_OFFSET = 0;
    static final int LENGTH_OFFSET = 2;
    static final int FIXED_DATA_SIZE = 4;

    private final int index;
    private final byte[] buffer;

    public ExtraData(byte[] buffer, int index)
    {
        if (index >= buffer.length) {
            throw new IllegalArgumentException("index past end of buffer");
        }
        if (buffer.length - index < FIXED_DATA_SIZE) {
            throw new IllegalArgumentException("incomplete extra data entry in buffer");
        }
        int length = ZipUtil.getUnsignedShort(buffer, index + LENGTH_OFFSET);
        if (buffer.length - index - FIXED_DATA_SIZE < length) {
            throw new IllegalArgumentException("incomplete extra data entry in buffer");
        }
        this.buffer = buffer;
        this.index = index;
    }

    public short getId()
    {
        return ZipUtil.get16(buffer, index + ID_OFFSET);
    }

    public int getLength()
    {
        return ZipUtil.getUnsignedShort(buffer, index + LENGTH_OFFSET) + FIXED_DATA_SIZE;
    }

    public byte[] getData()
    {
        return Arrays.copyOfRange(buffer, index + FIXED_DATA_SIZE, index + getLength());
    }

    public byte getByte(int index)
    {
        return buffer[this.index + index];
    }
}
