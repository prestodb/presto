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
package com.facebook.presto.orc.stream;

import com.facebook.presto.orc.OrcCorruptionException;
import com.facebook.presto.orc.checkpoint.DecimalStreamCheckpoint;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;

import java.io.IOException;

import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.rescale;
import static java.lang.Long.MAX_VALUE;

public class DecimalInputStream
        implements ValueInputStream<DecimalStreamCheckpoint>
{
    private final OrcInputStream input;

    public DecimalInputStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<? extends DecimalStreamCheckpoint> getCheckpointType()
    {
        return DecimalStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(DecimalStreamCheckpoint checkpoint)
            throws IOException
    {
        input.seekToCheckpoint(checkpoint.getInputStreamCheckpoint());
    }

    public void nextLongDecimal(Slice result)
            throws IOException
    {
        long b;
        long offset = 0;
        long low = 0;
        long high = 0;
        do {
            b = input.read();
            if (offset == 126 && ((b & 0x80) > 0 || (b & 0x7f) > 3)) {
                throw new OrcCorruptionException("Decimal exceeds 128 bits");
            }

            if (offset < 63) {
                low |= (b & 0x7f) << offset;
            }
            else if (offset == 63) {
                low |= (b & 0x01) << offset;
                high |= (b & 0x7f) >>> 1;
            }
            else {
                high |= (b & 0x7f) << (offset - 64);
            }
            offset += 7;
        }
        while ((b & 0x80) > 0);

        boolean negative = (low & 0x01) == 1;

        // drop sign bit
        low >>>= 1;
        low |= ((high & 0x1) << 63);
        high >>>= 1;

        // increment for negative values
        if (negative) {
            if (low == 0xFFFFFFFFFFFFFFFFL) {
                low = 0;
                high += 1;
            }
            else {
                low += 1;
            }
        }

        UnscaledDecimal128Arithmetic.pack(low, high, negative, result);
    }

    public void nextLongDecimalVector(int items, BlockBuilder builder, DecimalType targetType, long[] sourceScale)
            throws IOException
    {
        Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
        for (int i = 0; i < items; i++) {
            nextLongDecimal(decimal);
            rescale(decimal, (int) (targetType.getScale() - sourceScale[i]), rescaledDecimal);
            targetType.writeSlice(builder, rescaledDecimal);
        }
    }

    public void nextLongDecimalVector(int items, BlockBuilder builder, DecimalType targetType, long[] sourceScale, boolean[] isNull)
            throws IOException
    {
        Slice decimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
        Slice rescaledDecimal = UnscaledDecimal128Arithmetic.unscaledDecimal();
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                nextLongDecimal(decimal);
                rescale(decimal, (int) (targetType.getScale() - sourceScale[i]), rescaledDecimal);
                targetType.writeSlice(builder, rescaledDecimal);
            }
            else {
                builder.appendNull();
            }
        }
    }

    public long nextLong()
            throws IOException
    {
        long result = 0;
        int offset = 0;
        long b;
        do {
            b = input.read();
            if (b == -1) {
                throw new OrcCorruptionException("Reading BigInteger past EOF from " + input);
            }
            long work = 0x7f & b;
            if (offset >= 63 && (offset != 63 || work > 1)) {
                throw new OrcCorruptionException("Decimal does not fit long (invalid table schema?)");
            }
            result |= work << offset;
            offset += 7;
        }
        while (b >= 0x80);
        boolean isNegative = (result & 0x01) != 0;
        if (isNegative) {
            result += 1;
            result = -result;
            result = result >> 1;
            result |= 0x01L << 63;
        }
        else {
            result = result >> 1;
            result &= MAX_VALUE;
        }
        return result;
    }

    public void nextShortDecimalVector(int items, BlockBuilder builder, DecimalType targetType, long[] sourceScale)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            long value = nextLong();
            long rescaledDecimal = Decimals.rescale(value, (int) sourceScale[i], targetType.getScale());
            targetType.writeLong(builder, rescaledDecimal);
        }
    }

    public void nextShortDecimalVector(int items, BlockBuilder builder, DecimalType targetType, long[] sourceScale, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                long rescaledDecimal = Decimals.rescale(nextLong(), (int) sourceScale[i], targetType.getScale());
                targetType.writeLong(builder, rescaledDecimal);
            }
            else {
                builder.appendNull();
            }
        }
    }

    @Override
    public void skip(long items)
            throws IOException
    {
        while (items-- > 0) {
            int b;
            do {
                b = input.read();
                if (b == -1) {
                    throw new OrcCorruptionException("Reading BigInteger past EOF from " + input);
                }
            }
            while (b >= 0x80);
        }
    }
}
