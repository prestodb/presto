/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
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
package com.facebook.presto.slice;

import java.nio.ByteBuffer;

public final class VariableLengthQuantity
{
    private VariableLengthQuantity()
    {
    }

    public static int variableLengthSize(int value)
    {
        int size = 1;
        while ((value & (~0x7f)) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    public static int variableLengthSize(long value)
    {
        int size = 1;
        while ((value & (~0x7f)) != 0) {
            value >>>= 7;
            size++;
        }
        return size;
    }

    public static void writeVariableLengthInt(int value, SliceOutput sliceOutput)
    {
        int highBitMask = 0x80;
        if (value < (1 << 7) && value >= 0) {
            sliceOutput.writeByte(value);
        }
        else if (value < (1 << 14) && value > 0) {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte(value >>> 7);
        }
        else if (value < (1 << 21) && value > 0) {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte((value >>> 7) | highBitMask);
            sliceOutput.writeByte(value >>> 14);
        }
        else if (value < (1 << 28) && value > 0) {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte((value >>> 7) | highBitMask);
            sliceOutput.writeByte((value >>> 14) | highBitMask);
            sliceOutput.writeByte(value >>> 21);
        }
        else {
            sliceOutput.writeByte(value | highBitMask);
            sliceOutput.writeByte((value >>> 7) | highBitMask);
            sliceOutput.writeByte((value >>> 14) | highBitMask);
            sliceOutput.writeByte((value >>> 21) | highBitMask);
            sliceOutput.writeByte(value >>> 28);
        }
    }

    public static void writeVariableLengthLong(long value, SliceOutput sliceOutput)
    {
        // while value more than the first 7 bits set
        while ((value & (~0x7f)) != 0) {
            sliceOutput.writeByte((int) ((value & 0x7f) | 0x80));
            value >>>= 7;
        }
        sliceOutput.writeByte((int) value);
    }

    public static int readVariableLengthInt(SliceInput sliceInput)
    {
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = sliceInput.readUnsignedByte();

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }

    public static int readVariableLengthInt(ByteBuffer sliceInput)
    {
        int result = 0;
        for (int shift = 0; shift <= 28; shift += 7) {
            int b = sliceInput.get();

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }

    public static long readVariableLengthLong(SliceInput sliceInput)
    {
        long result = 0;
        for (int shift = 0; shift <= 63; shift += 7) {
            long b = sliceInput.readUnsignedByte();

            // add the lower 7 bits to the result
            result |= ((b & 0x7f) << shift);

            // if high bit is not set, this is the last byte in the number
            if ((b & 0x80) == 0) {
                return result;
            }
        }
        throw new NumberFormatException("last byte of variable length int has high bit set");
    }
}
