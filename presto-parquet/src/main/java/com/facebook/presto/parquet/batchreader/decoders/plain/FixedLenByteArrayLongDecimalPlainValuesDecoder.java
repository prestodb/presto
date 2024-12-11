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

import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.LongDecimalValuesDecoder;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class FixedLenByteArrayLongDecimalPlainValuesDecoder
        implements LongDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BinaryLongDecimalPlainValuesDecoder.class).instanceSize();

    private final int typeLength;
    private final byte[] inputBytes;
    private final byte[] byteBuffer;
    private final int bufferEnd;

    private int bufferOffset;

    public FixedLenByteArrayLongDecimalPlainValuesDecoder(int typeLength, byte[] byteBuffer, int bufferOffset, int length)
    {
        checkArgument(typeLength > 0 && typeLength <= 16, "typeLength %s should be in range (1-16) for a long decimal", typeLength);
        this.typeLength = typeLength;
        this.inputBytes = new byte[typeLength];
        this.byteBuffer = requireNonNull(byteBuffer, "byteBuffer is null");
        this.bufferOffset = bufferOffset;
        this.bufferEnd = bufferOffset + length;
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        int localBufferOffset = bufferOffset;
        int endOffset = (offset + length) * 2;

        for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
            System.arraycopy(byteBuffer, localBufferOffset, inputBytes, 0, typeLength);
            Slice slice = Decimals.encodeUnscaledValue(new BigInteger(inputBytes));
            values[currentOutputOffset] = slice.getLong(0);
            values[currentOutputOffset + 1] = slice.getLong(SIZE_OF_LONG);

            localBufferOffset += typeLength;
        }

        bufferOffset = localBufferOffset;
    }

    @Override
    public void skip(int length)
    {
        checkArgument(bufferOffset + length * typeLength <= bufferEnd, "End of stream: invalid read request");
        checkArgument(length >= 0, "invalid length %s", length);
        bufferOffset += length * typeLength;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(byteBuffer) + sizeOf(inputBytes);
    }
}
