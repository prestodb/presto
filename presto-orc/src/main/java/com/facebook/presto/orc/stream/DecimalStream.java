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
import io.airlift.slice.Slice;

import java.io.IOException;
import java.math.BigInteger;

import static java.lang.Long.MAX_VALUE;

public class DecimalStream
        implements ValueStream<DecimalStreamCheckpoint>
{
    private final OrcInputStream input;

    public DecimalStream(OrcInputStream input)
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

    // This comes from the Apache Hive ORC code (see org.apache.hadoop.hive.ql.io.orc.SerializationUtils.java)
    public BigInteger nextBigInteger()
            throws IOException
    {
        BigInteger result = BigInteger.ZERO;
        long work = 0;
        int offset = 0;
        long b;
        do {
            b = input.read();
            if (b == -1) {
                throw new OrcCorruptionException("Reading BigInteger past EOF from " + input);
            }
            work |= (0x7f & b) << (offset % 63);
            if (offset >= 126 && (offset != 126 || work > 3)) {
                throw new OrcCorruptionException("Decimal exceeds 128 bits");
            }
            offset += 7;
            // if we've read 63 bits, roll them into the result
            if (offset == 63) {
                result = BigInteger.valueOf(work);
                work = 0;
            }
            else if (offset % 63 == 0) {
                result = result.or(BigInteger.valueOf(work).shiftLeft(offset - 63));
                work = 0;
            }
        }
        while (b >= 0x80);
        if (work != 0) {
            result = result.or(BigInteger.valueOf(work).shiftLeft((offset / 63) * 63));
        }
        // convert back to a signed number
        boolean isNegative = result.testBit(0);
        if (isNegative) {
            result = result.add(BigInteger.ONE);
            result = result.negate();
        }
        result = result.shiftRight(1);
        return result;
    }

    public void nextLongDecimalVector(int items, BlockBuilder builder, DecimalType targetType, long[] sourceScale)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            BigInteger bigInteger = nextBigInteger();
            BigInteger rescaledDecimal = Decimals.rescale(bigInteger, (int) sourceScale[i], targetType.getScale());
            Slice slice = Decimals.encodeUnscaledValue(rescaledDecimal);
            targetType.writeSlice(builder, slice);
        }
    }

    public void nextLongDecimalVector(int items, BlockBuilder builder, DecimalType targetType, long[] sourceScale, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                BigInteger bigInteger = nextBigInteger();
                BigInteger rescaledDecimal = Decimals.rescale(bigInteger, (int) sourceScale[i], targetType.getScale());
                Slice slice = Decimals.encodeUnscaledValue(rescaledDecimal);
                targetType.writeSlice(builder, slice);
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
            long rescaledDecimal = Decimals.rescale(nextLong(), (int) sourceScale[i], targetType.getScale());
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
