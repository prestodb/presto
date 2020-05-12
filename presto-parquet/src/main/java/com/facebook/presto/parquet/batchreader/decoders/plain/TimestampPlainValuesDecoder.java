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
package com.facebook.presto.parquet.batchreader.decoders.plain;

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.TimestampValuesDecoder;

import static com.facebook.presto.parquet.ParquetTimestampUtils.getTimestampMillis;
import static com.google.common.base.Preconditions.checkArgument;

public class TimestampPlainValuesDecoder
        implements TimestampValuesDecoder
{
    private final byte[] byteBuffer;
    private final int bufferEnd;

    private int bufferOffset;

    public TimestampPlainValuesDecoder(byte[] byteBuffer, int bufferOffset, int bufferLength)
    {
        this.byteBuffer = byteBuffer;
        this.bufferEnd = bufferOffset + bufferLength;
        this.bufferOffset = bufferOffset;
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        checkArgument(bufferOffset + length * 12 <= bufferEnd, "End of stream: invalid read request");
        checkArgument(length >= 0 && offset >= 0, "invalid read request: offset %s, length", offset, length);

        final int endOffset = offset + length;
        final byte[] localByteBuffer = byteBuffer;
        int localBufferOffset = bufferOffset;

        while (offset < endOffset) {
            values[offset++] = getTimestampMillis(localByteBuffer, localBufferOffset);
            localBufferOffset += 12;
        }

        bufferOffset = localBufferOffset;
    }

    @Override
    public void skip(int length)
    {
        checkArgument(bufferOffset + length * 12 <= bufferEnd, "End of stream: invalid read request");
        checkArgument(length >= 0, "invalid length %s", length);

        bufferOffset += length * 12;
    }
}
