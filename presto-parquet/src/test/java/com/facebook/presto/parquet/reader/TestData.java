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
package com.facebook.presto.parquet.reader;

import com.facebook.presto.common.type.Decimals;
import com.google.common.primitives.Longs;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Random;
import java.util.function.IntFunction;

import static com.facebook.presto.parquet.batchreader.BytesUtils.propagateSignBit;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.Math.toIntExact;

public final class TestData
{
    private TestData() {}

    // Based on org.apache.parquet.schema.Types.BasePrimitiveBuilder.maxPrecision to determine the max decimal precision supported by INT32/INT64
    public static int maxPrecision(int numBytes)
    {
        return toIntExact(
                // convert double to long
                Math.round(
                        // number of base-10 digits
                        Math.floor(Math.log10(
                                Math.pow(2, 8 * numBytes - 1) - 1))));  // max value stored in numBytes
    }

    public static IntFunction<long[]> unscaledRandomShortDecimalSupplier(int bitWidth, int precision)
    {
        long min = (-1 * Decimals.longTenToNth(precision)) + 1;
        long max = Decimals.longTenToNth(precision) - 1;
        return size -> {
            long[] result = new long[size];
            for (int i = 0; i < size; i++) {
                result[i] = Math.max(
                        Math.min(generateData(bitWidth), max),
                        min);
            }
            return result;
        };
    }

    public static byte[] longToBytes(long value, int length)
    {
        byte[] result = new byte[length];
        for (int i = length - 1; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= Byte.SIZE;
        }
        return result;
    }

    public static Slice randomBigInteger(Random r)
    {
        byte[] result = new byte[2 * SIZE_OF_LONG];
        byte[] high = randomLong(r, 0, 0x4b3b4ca85a86c47aL);
        System.arraycopy(high, 0, result, 0, high.length);
        byte[] low = randomLong(r, 0, 0x98a2240000000000L);
        System.arraycopy(low, 0, result, 2 * SIZE_OF_LONG - high.length, low.length);
        return Slices.wrappedBuffer(result);
    }

    private static byte[] randomLong(Random r, long min, long max)
    {
        return Longs.toByteArray(r.nextLong() % (max - min) + min);
    }

    public static long generateData(int bitWidth)
    {
        checkArgument(bitWidth <= 64 && bitWidth > 0, "bit width must be in range 1 - 64 inclusive");
        if (bitWidth == 64) {
            return 10;
        }
        return propagateSignBit(2, 64 - bitWidth);
    }
}
