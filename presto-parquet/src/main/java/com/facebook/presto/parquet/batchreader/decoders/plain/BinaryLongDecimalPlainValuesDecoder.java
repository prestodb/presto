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
import com.facebook.presto.parquet.batchreader.decoders.plain.BinaryPlainValuesDecoder.PlainValueBuffer;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;
import java.util.Arrays;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class BinaryLongDecimalPlainValuesDecoder
        implements LongDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(BinaryLongDecimalPlainValuesDecoder.class).instanceSize();

    private final BinaryPlainValuesDecoder delegate;

    public BinaryLongDecimalPlainValuesDecoder(byte[] buffer, int bufOffset, int bufLength)
    {
        requireNonNull(buffer, "buffer is null");
        delegate = new BinaryPlainValuesDecoder(buffer, bufOffset, bufLength);
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        PlainValueBuffer valueBuffer = (PlainValueBuffer) delegate.readNext(length);
        int bufferSize = valueBuffer.getBufferSize();
        byte[] byteBuffer = new byte[bufferSize];
        int[] offsets = new int[length + 1];
        delegate.readIntoBuffer(byteBuffer, 0, offsets, 0, valueBuffer);

        for (int i = 0; i < length; i++) {
            int positionOffset = offsets[i];
            int positionLength = offsets[i + 1] - positionOffset;
            byte[] temp = Arrays.copyOfRange(byteBuffer, positionOffset, positionOffset + positionLength);
            Slice slice = Decimals.encodeUnscaledValue(new BigInteger(temp));
            values[2 * (offset + i)] = slice.getLong(0);
            values[2 * (offset + i) + 1] = slice.getLong(SIZE_OF_LONG);
        }
    }

    @Override
    public void skip(int length)
    {
        checkArgument(length >= 0, "invalid length %s", length);
        delegate.skip(length);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
