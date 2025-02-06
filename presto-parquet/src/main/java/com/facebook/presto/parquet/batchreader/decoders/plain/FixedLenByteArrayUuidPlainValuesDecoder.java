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

import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.UuidValuesDecoder;
import org.openjdk.jol.info.ClassLayout;

import java.nio.ByteBuffer;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.nio.ByteOrder.BIG_ENDIAN;

public class FixedLenByteArrayUuidPlainValuesDecoder
        implements UuidValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedLenByteArrayUuidPlainValuesDecoder.class).instanceSize();

    private final int typeLength;
    private final int bufferEnd;
    private final ByteBuffer buffer;

    public FixedLenByteArrayUuidPlainValuesDecoder(int typeLength, byte[] buffer, int baseOffset, int length)
    {
        checkArgument(typeLength == 16, "typeLength %s should be 16 for a UUID", typeLength);
        this.typeLength = typeLength;
        this.buffer = ByteBuffer.wrap(buffer).order(BIG_ENDIAN);
        this.buffer.position(baseOffset);
        this.bufferEnd = baseOffset + length;
    }

    /**
     * @param values array containing decoded values
     * @param offset offset to start writing in the values argument
     * @param length number of values to write
     */
    @Override
    public void readNext(long[] values, int offset, int length)
    {
        int readEndOffset = (offset + length) * 2;

        for (int currentOutputOffset = offset * 2; currentOutputOffset < readEndOffset; currentOutputOffset += 2) {
            values[currentOutputOffset] = buffer.getLong();
            values[currentOutputOffset + 1] = buffer.getLong();
        }
    }

    @Override
    public void skip(int length)
    {
        checkArgument(buffer.position() + (length * typeLength) <= bufferEnd, "End of stream: invalid read request");
        checkArgument(length >= 0, "invalid length %s", length);
        buffer.position(buffer.position() + (length * typeLength));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(buffer.array());
    }
}
