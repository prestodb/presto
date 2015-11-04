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

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigInteger;

import static com.facebook.presto.orc.metadata.CompressionKind.UNCOMPRESSED;
import static java.lang.Long.MAX_VALUE;
import static java.lang.Long.MIN_VALUE;
import static java.math.BigInteger.ONE;
import static org.testng.Assert.assertEquals;

public class TestUnboundedIntegerStream
{
    @Test
    public void testShortUnboundedIntegers()
            throws IOException
    {
        assertReadsShortValue(0L);
        assertReadsShortValue(1L);
        assertReadsShortValue(-1L);
        assertReadsShortValue(256L);
        assertReadsShortValue(-256L);
        assertReadsShortValue(MAX_VALUE);
        assertReadsShortValue(MIN_VALUE);
    }

    @Test(expectedExceptions = {IllegalStateException.class})
    public void testShouldFailWhenValueDoesNotFit()
            throws IOException
    {
        UnboundedIntegerStream stream = new UnboundedIntegerStream(unboundedIntegerInputStream(BigInteger.valueOf(MAX_VALUE).add(ONE)));
        stream.nextLong();
    }

    @Test
    public void testLongUnboundedIntegers()
            throws IOException
    {
        assertReadsLongValue(BigInteger.valueOf(0L));
        assertReadsLongValue(BigInteger.valueOf(1L));
        assertReadsLongValue(BigInteger.valueOf(-1L));
        assertReadsLongValue(BigInteger.valueOf(MAX_VALUE).pow(123));
        assertReadsLongValue(BigInteger.valueOf(MIN_VALUE).pow(123));
    }

    @Test
    public void testSkipsValue()
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeBigInteger(baos, BigInteger.valueOf(MAX_VALUE));
        writeBigInteger(baos, BigInteger.valueOf(MIN_VALUE));

        OrcInputStream inputStream = orcInputStreamFor("skip test", baos.toByteArray());
        UnboundedIntegerStream stream = new UnboundedIntegerStream(inputStream);
        stream.skip(1);
        assertEquals(stream.nextLong(), MIN_VALUE);
    }

    private static void assertReadsShortValue(long value)
            throws IOException
    {
        UnboundedIntegerStream stream = new UnboundedIntegerStream(unboundedIntegerInputStream(BigInteger.valueOf(value)));
        assertEquals(stream.nextLong(), value);
    }

    private static void assertReadsLongValue(BigInteger value)
            throws IOException
    {
        UnboundedIntegerStream stream = new UnboundedIntegerStream(unboundedIntegerInputStream(value));
        assertEquals(stream.nextBigInteger(), value);
    }

    private static OrcInputStream unboundedIntegerInputStream(BigInteger value)
            throws IOException
    {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        writeBigInteger(baos, value);
        return orcInputStreamFor(value.toString(), baos.toByteArray());
    }

    private static OrcInputStream orcInputStreamFor(String source, byte[] bytes)
    {
        return new OrcInputStream(source, new BasicSliceInput(Slices.wrappedBuffer(bytes)), UNCOMPRESSED, 10000);
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
