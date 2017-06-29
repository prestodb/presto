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
package com.facebook.presto.plugin.turbonium.encodings;

import com.facebook.presto.plugin.turbonium.storage.Values;

import java.util.BitSet;
import java.util.Optional;

public class DeltaValuesBuilder
{
    private static final short BYTE_WIDTH = (short) 0xff;
    private static final byte BOOL_WIDTH = (byte) 1;
    private static final int SHORT_WIDTH = 0xffff;
    private static final long INT_WIDTH = 0xffff_ffffL;

    private  DeltaValuesBuilder() {}

    public static Optional<Values> buildLongValues(long min, long delta, long[] values, int size)
    {
        long width = Long.highestOneBit(delta);
        Optional<Values> deltaValues = Optional.empty();
        if (width <= BOOL_WIDTH) {
            BitSet bitSet = new BitSet(size);
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                if (value != 0) {
                    bitSet.set(position);
                }
            }
            deltaValues = Optional.of(new Values.BooleanValues(bitSet));
        }
        else if (width <= BYTE_WIDTH) {
            byte[] deltas = new byte[size];
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                deltas[position] = (byte) value;
            }
            deltaValues = Optional.of(new Values.ByteValues(deltas));
        }
        else if (width <= SHORT_WIDTH) {
            short[] deltas = new short[size];
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                deltas[position] = (short) value;
            }
            deltaValues = Optional.of(new Values.ShortValues(deltas));
        }
        else if (width <= INT_WIDTH) {
            int[] deltas = new int[size];
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                deltas[position] = (int) value;
            }
            deltaValues = Optional.of(new Values.IntValues(deltas));
        }
        return deltaValues;
    }

    public static Optional<Values> buildIntValues(int min, int delta, int[] values, int size)
    {
        int width = Integer.highestOneBit(delta);
        Optional<Values> deltaValues = Optional.empty();
        if (width <= BOOL_WIDTH) {
            BitSet bitSet = new BitSet(size);
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                if (value != 0) {
                    bitSet.set(position);
                }
            }
            deltaValues = Optional.of(new Values.BooleanValues(bitSet));
        }
        else if (width <= BYTE_WIDTH) {
            byte[] deltas = new byte[size];
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                deltas[position] = (byte) value;
            }
            deltaValues = Optional.of(new Values.ByteValues(deltas));
        }
        else if (width <= SHORT_WIDTH) {
            short[] deltas = new short[size];
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                deltas[position] = (short) value;
            }
            deltaValues = Optional.of(new Values.ShortValues(deltas));
        }
        return deltaValues;
    }

    public static Optional<Values> buildShortValues(short min, short delta, short[] values, int size)
    {
        int width = Integer.highestOneBit(0xffff & delta);
        Optional<Values> deltaValues = Optional.empty();
        if (width <= BOOL_WIDTH) {
            BitSet bitSet = new BitSet(size);
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                if (value != 0) {
                    bitSet.set(position);
                }
            }
            deltaValues = Optional.of(new Values.BooleanValues(bitSet));
        }
        else if (width <= BYTE_WIDTH) {
            byte[] deltas = new byte[size];
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                deltas[position] = (byte) value;
            }
            deltaValues = Optional.of(new Values.ByteValues(deltas));
        }
        return deltaValues;
    }

    public static Optional<Values> buildByteValues(byte min, byte delta, byte[] values, int size)
    {
        int width = Integer.highestOneBit(0xff & delta);
        Optional<Values> deltaValues = Optional.empty();
        if (width <= BOOL_WIDTH) {
            BitSet bitSet = new BitSet(size);
            for (int position = 0; position < size; position++) {
                long value = values[position] - min;
                if (value != 0) {
                    bitSet.set(position);
                }
            }
            deltaValues = Optional.of(new Values.BooleanValues(bitSet));
        }
        return deltaValues;
    }
}
