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

import com.facebook.presto.orc.checkpoint.UnboundedIntegerStreamCheckpoint;

import java.io.EOFException;
import java.io.IOException;
import java.math.BigInteger;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.Long.MAX_VALUE;

public class UnboundedIntegerStream
        implements ValueStream<UnboundedIntegerStreamCheckpoint>
{
    private final OrcInputStream input;

    public UnboundedIntegerStream(OrcInputStream input)
    {
        this.input = input;
    }

    @Override
    public Class<? extends UnboundedIntegerStreamCheckpoint> getCheckpointType()
    {
        return UnboundedIntegerStreamCheckpoint.class;
    }

    @Override
    public void seekToCheckpoint(UnboundedIntegerStreamCheckpoint checkpoint)
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
                throw new EOFException("Reading BigInteger past EOF from " + input);
            }
            work |= (0x7f & b) << (offset % 63);
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

    public void nextBigIntegerVector(int items, BigInteger[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = nextBigInteger();
        }
    }

    public void nextBigIntegerVector(int items, BigInteger[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = nextBigInteger();
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
                throw new EOFException("Reading BigInteger past EOF from " + input);
            }
            long work = 0x7f & b;
            checkState(offset <= 56 || (offset == 63 && work <= 1), "Unbounded integer does not fit long");
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

    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = nextLong();
        }
    }

    public void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = nextLong();
            }
        }
    }

    @Override
    public void skip(int items)
            throws IOException
    {
        while (items-- > 0) {
            int b;
            do {
                b = input.read();
                if (b == -1) {
                    throw new EOFException("Reading BigInteger past EOF from " + input);
                }
            }
            while (b >= 0x80);
        }
    }
}
