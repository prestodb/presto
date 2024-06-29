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
package com.facebook.presto.parquet.batchreader.decoders.delta;

import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.parquet.batchreader.decoders.ValuesDecoder.LongDecimalValuesDecoder;
import io.airlift.slice.Slice;
import org.apache.parquet.column.values.ValuesReader;
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

/**
 * Note: this is not an optimized values decoder. It makes use of the existing Parquet decoder. Given that this type encoding
 * is not a common one, just use the existing one provided by Parquet library and add a wrapper around it that satisfies the
 * {@link LongDecimalValuesDecoder} interface.
 */
public class FixedLenByteArrayLongDecimalDeltaValueDecoder
        implements LongDecimalValuesDecoder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FixedLenByteArrayLongDecimalDeltaValueDecoder.class).instanceSize();

    private final ValuesReader delegate;

    public FixedLenByteArrayLongDecimalDeltaValueDecoder(ValuesReader delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
    }

    @Override
    public void readNext(long[] values, int offset, int length)
    {
        int endOffset = (offset + length) * 2;
        for (int currentOutputOffset = offset * 2; currentOutputOffset < endOffset; currentOutputOffset += 2) {
            byte[] inputBytes = delegate.readBytes().getBytes();
            Slice slice = Decimals.encodeUnscaledValue(new BigInteger(inputBytes));
            values[currentOutputOffset] = slice.getLong(0);
            values[currentOutputOffset + 1] = slice.getLong(SIZE_OF_LONG);
        }
    }

    @Override
    public void skip(int length)
    {
        delegate.skip(length);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE;
    }
}
