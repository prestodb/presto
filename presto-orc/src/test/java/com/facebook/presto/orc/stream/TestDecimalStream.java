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
import com.facebook.presto.orc.OrcDataSourceId;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;
import java.util.Optional;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.spi.type.Decimals.MAX_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.Decimals.MIN_DECIMAL_UNSCALED_VALUE;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimal;
import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.unscaledDecimalToBigInteger;
import static java.math.BigInteger.ONE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestDecimalStream
{
    private static final BigInteger BIG_INTEGER_127_BIT_SET;

    static {
        BigInteger b = BigInteger.ZERO;
        for (int i = 0; i < 127; ++i) {
            b = b.setBit(i);
        }
        BIG_INTEGER_127_BIT_SET = b;
    }

    @Test
    public void testShortDecimals()
            throws IOException
    {
        assertReadsShortValue(0L);
        assertReadsShortValue(1L);
        assertReadsShortValue(-1L);
        assertReadsShortValue(256L);
        assertReadsShortValue(-256L);
        assertReadsShortValue(Long.MAX_VALUE);
        assertReadsShortValue(Long.MIN_VALUE);
    }

    @Test
    public void testShouldFailWhenShortDecimalDoesNotFit()
    {
        assertShortValueReadFails(BigInteger.valueOf(Long.MAX_VALUE).add(ONE));
    }

    @Test
    public void testShouldFailWhenExceeds128Bits()
    {
        assertLongValueReadFails(BigInteger.valueOf(1).shiftLeft(127));
        assertLongValueReadFails(BigInteger.valueOf(-2).shiftLeft(127));
    }

    @Test
    public void testLongDecimals()
            throws IOException
    {
        assertReadsLongValue(BigInteger.valueOf(0L));
        assertReadsLongValue(BigInteger.valueOf(1L));
        assertReadsLongValue(BigInteger.valueOf(-1L));
        assertReadsLongValue(BigInteger.valueOf(-1).shiftLeft(126));
        assertReadsLongValue(BigInteger.valueOf(1).shiftLeft(126));
        assertReadsLongValue(BIG_INTEGER_127_BIT_SET);
        assertReadsLongValue(BIG_INTEGER_127_BIT_SET.negate());
        assertReadsLongValue(MAX_DECIMAL_UNSCALED_VALUE);
        assertReadsLongValue(MIN_DECIMAL_UNSCALED_VALUE);
    }

    @Test
    public void testSkipsValue()
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeBigInteger(baos, BigInteger.valueOf(Long.MAX_VALUE));
        writeBigInteger(baos, BigInteger.valueOf(Long.MIN_VALUE));

        OrcChunkLoader chunkLoader = orcChunkLoaderFor("skip test", baos.toByteArray());
        DecimalInputStream stream = new DecimalInputStream(chunkLoader);
        stream.skip(1);
        assertEquals(nextShortDecimalValue(stream), Long.MIN_VALUE);
    }

    private static void assertReadsShortValue(long value)
            throws IOException
    {
        DecimalInputStream stream = new DecimalInputStream(decimalChunkLoader(BigInteger.valueOf(value)));
        assertEquals(nextShortDecimalValue(stream), value);
    }

    private static void assertReadsLongValue(BigInteger value)
            throws IOException
    {
        DecimalInputStream stream = new DecimalInputStream(decimalChunkLoader(value));
        assertEquals(nextLongDecimalValue(stream), value);
    }

    private static void assertShortValueReadFails(BigInteger value)
    {
        assertThrows(OrcCorruptionException.class, () -> {
            DecimalInputStream stream = new DecimalInputStream(decimalChunkLoader(value));
            nextShortDecimalValue(stream);
        });
    }

    private static void assertLongValueReadFails(BigInteger value)
    {
        Slice decimal = unscaledDecimal();
        assertThrows(OrcCorruptionException.class, () -> {
            DecimalInputStream stream = new DecimalInputStream(decimalChunkLoader(value));
            nextLongDecimalValue(stream);
        });
    }

    private static long nextShortDecimalValue(DecimalInputStream stream)
            throws IOException
    {
        long[] values = new long[1];
        stream.nextShortDecimal(values, 1);
        return values[0];
    }

    private static BigInteger nextLongDecimalValue(DecimalInputStream stream)
            throws IOException
    {
        long[] decimal = new long[2];
        stream.nextLongDecimal(decimal, 1);
        return unscaledDecimalToBigInteger(Slices.wrappedLongArray(decimal));
    }

    private static OrcChunkLoader decimalChunkLoader(BigInteger value)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeBigInteger(baos, value);
        return orcChunkLoaderFor(value.toString(), baos.toByteArray());
    }

    private static OrcChunkLoader orcChunkLoaderFor(String source, byte[] bytes)
    {
        return OrcChunkLoader.create(new OrcDataSourceId(source), Slices.wrappedBuffer(bytes), Optional.empty(), newSimpleAggregatedMemoryContext());
    }

    // copied from org.apache.hadoop.hive.ql.io.orc.SerializationUtils.java
    private static void writeBigInteger(OutputStream output, BigInteger value)
            throws IOException
    {
        // encode the signed number as a positive integer
        value = value.shiftLeft(1);
        int sign = value.signum();
        if (sign < 0) {
            value = value.negate();
            value = value.subtract(ONE);
        }
        int length = value.bitLength();
        while (true) {
            long lowBits = value.longValue() & 0x7fffffffffffffffL;
            length -= 63;
            // write out the next 63 bits worth of data
            for (int i = 0; i < 9; ++i) {
                // if this is the last byte, leave the high bit off
                if (length <= 0 && (lowBits & ~0x7f) == 0) {
                    output.write((byte) lowBits);
                    return;
                }
                else {
                    output.write((byte) (0x80 | (lowBits & 0x7f)));
                    lowBits >>>= 7;
                }
            }
            value = value.shiftRight(63);
        }
    }
}
