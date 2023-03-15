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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.Int32ValuesDecoder;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.parquet.batchreader.BytesUtils.getInt;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;

public class Int32PlainValuesDecoder
        implements Int32ValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Int32PlainValuesDecoder.class).instanceSize();

    private final byte[] byteBuffer;
    private final int bufferEnd;

    private int bufferOffset;

    public Int32PlainValuesDecoder(byte[] byteBuffer, int bufferOffset, int length)
    {
        this.byteBuffer = byteBuffer;
        this.bufferOffset = bufferOffset;
        this.bufferEnd = bufferOffset + length;
    }

    @Override
    public void readNext(int[] values, int offset, int length)
    {
        checkArgument(bufferOffset + length * 4 <= bufferEnd, "End of stream: invalid read request");
        checkArgument(length >= 0 && offset >= 0, "invalid read request: offset %s, length", offset, length);

        final int endOffset = offset + length;
        final byte[] localByteBuffer = byteBuffer;
        int localBufferOffset = bufferOffset;

        while (offset < endOffset) {
            values[offset++] = getInt(localByteBuffer, localBufferOffset);
            localBufferOffset += 4;
        }

        bufferOffset = localBufferOffset;
    }

    @Override
    public void skip(int length)
    {
        checkArgument(bufferOffset + length * 4 <= bufferEnd, "End of stream: invalid read request");
        checkArgument(length >= 0, "invalid length %s", length);
        bufferOffset += length * 4;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(byteBuffer);
    }
}
