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
package com.facebook.presto.hive.orc.stream;

import com.google.common.primitives.Ints;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import static com.facebook.presto.hive.orc.stream.OrcStreamUtils.MIN_REPEAT_SIZE;

public class LongStreamV1
        implements LongStream
{
    private static final int MAX_LITERAL_SIZE = 128;

    private final InputStream input;
    private final boolean signed;
    private final long[] literals = new long[MAX_LITERAL_SIZE];
    private int numLiterals;
    private int delta;
    private int used;
    private boolean repeat;

    public LongStreamV1(InputStream input, boolean signed)
            throws IOException
    {
        this.input = input;
        this.signed = signed;
    }

    private void readValues()
            throws IOException
    {
        int control = input.read();
        if (control == -1) {
            throw new EOFException("Read past end of RLE integer from " + input);
        }
        else if (control < 0x80) {
            numLiterals = control + MIN_REPEAT_SIZE;
            used = 0;
            repeat = true;
            delta = input.read();
            if (delta == -1) {
                throw new EOFException("End of stream in RLE Integer from " + input);
            }
            // convert from 0 to 255 to -128 to 127 by converting to a signed byte
            // noinspection SillyAssignment
            delta = (byte) delta;
            literals[0] = LongDecode.readVInt(signed, input);
        }
        else {
            numLiterals = 0x100 - control;
            used = 0;
            repeat = false;
            for (int i = 0; i < numLiterals; ++i) {
                literals[i] = LongDecode.readVInt(signed, input);
            }
        }
    }

    @Override
    public long next()
            throws IOException
    {
        long result;
        if (used == numLiterals) {
            readValues();
        }
        if (repeat) {
            result = literals[0] + (used++) * delta;
        }
        else {
            result = literals[used++];
        }
        return result;
    }

    @Override
    public void skip(int items)
            throws IOException
    {
        while (items > 0) {
            if (used == numLiterals) {
                readValues();
            }
            long consume = Math.min(items, numLiterals - used);
            used += consume;
            items -= consume;
        }
    }

    @Override
    public long sum(int items)
            throws IOException
    {
        long sum = 0;
        for (int i = 0; i < items; i++) {
            sum += next();
        }
        return sum;
    }

    @Override
    public void nextLongVector(int items, long[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = next();
        }
    }

    @Override
    public void nextLongVector(int items, long[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = next();
            }
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            vector[i] = Ints.checkedCast(next());
        }
    }

    @Override
    public void nextIntVector(int items, int[] vector, boolean[] isNull)
            throws IOException
    {
        for (int i = 0; i < items; i++) {
            if (!isNull[i]) {
                vector[i] = Ints.checkedCast(next());
            }
        }
    }
}
