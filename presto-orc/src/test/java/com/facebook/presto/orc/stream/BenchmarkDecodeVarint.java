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

import io.airlift.slice.ByteArrays;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkDecodeVarint
{
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;
    private static final byte[] ENCODED_DATA;
    private static final long[] DATA;
    private static final int LENGTH;
    private static final int COUNT = 1_000_000;

    static {
        DATA = getData();
        ENCODED_DATA = new byte[DATA.length * 10];
        LENGTH = getEncoded(DATA, ENCODED_DATA);
    }

    @Benchmark
    @OperationsPerInvocation(10)
    public Object benchmarkDecodeVarints(BenchmarkData data)
    {
        return data.varintDecoder.decodeVarint(data.destination, data.data, LENGTH);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"false", "true"})
        boolean useUnsafe;

        byte[] data;
        VarintDecoder varintDecoder;
        long[] destination;
        @Setup
        public void setup()
        {
            this.data = ENCODED_DATA;
            this.destination = new long[COUNT];
            if (useUnsafe) {
                varintDecoder = new UnsafeVarintDecoder();
            }
            else {
                varintDecoder = new SafeVarintDecoder();
            }
        }
    }

    public static class UnsafeVarintDecoder
            implements VarintDecoder
    {
        public long decodeVarint(long[] destination, byte[] array, int arrayLength)
        {
            int position = 0;
            int offset = 0;
            long result = 0;
            while (offset + SIZE_OF_LONG + 2 < arrayLength) {
                long value = ByteArrays.getLong(array, offset);
                offset += SIZE_OF_LONG;
                long mask = (value & VARINT_MASK) ^ VARINT_MASK;
                if (mask == 0) {
                    result = decode1(value, 8);
                    result |= (long) (array[offset] & 0x7f) << 56;
                    if ((array[offset++] & 0x80) != 0) {
                        if ((array[offset] & 0x80) != 0) {
                            throw new IllegalStateException("invalid format");
                        }
                        result |= (long) array[offset++] << 63;
                    }
                }
                else {
                    int count = (Long.numberOfTrailingZeros(mask) + 1) >>> 3;
                    result = decode1(value, count);
                }
                destination[position++] = result;
            }
            result = 0;
            for (int shift = 0; offset < arrayLength; offset++) {
                result |= (long) (array[offset] & 0x7f) << shift;
                if ((array[offset] & 0x80) == 0) {
                    destination[position++] = result;
                    result = 0;
                    shift = 0;
                }
                else {
                    shift += 7;
                }
            }
            return position;
        }

        private long decode1(long value, int count)
        {
            switch (count) {
                case 1:
                   return (value & 0x7f);
                case 2:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1);
                case 3:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1) |
                            ((value & 0x7f_0000L) >> 2);
                case 4:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1) |
                            ((value & 0x7f_0000L) >> 2) |
                            ((value & 0x7f00_0000L) >> 3);
                case 5:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1) |
                            ((value & 0x7f_0000L) >> 2) |
                            ((value & 0x7f00_0000L) >> 3) |
                            ((value & 0x7f_0000_0000L) >> 4);
                case 6:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1) |
                            ((value & 0x7f_0000L) >> 2) |
                            ((value & 0x7f00_0000L) >> 3) |
                            ((value & 0x7f_0000_0000L) >> 4)|
                            ((value & 0x7f00_0000_0000L) >> 5);
                case 7:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1) |
                            ((value & 0x7f_0000L) >> 2) |
                            ((value & 0x7f00_0000L) >> 3) |
                            ((value & 0x7f_0000_0000L) >> 4) |
                            ((value & 0x7f00_0000_0000L) >> 5) |
                            ((value & 0x7f_0000_0000_0000L) >> 6);
                case 8:
                    return (value & 0x7f) |
                            ((value & 0x7f00L) >> 1) |
                            ((value & 0x7f_0000L) >> 2) |
                            ((value & 0x7f00_0000L) >> 3) |
                            ((value & 0x7f_0000_0000L) >> 4) |
                            ((value & 0x7f00_0000_0000L) >> 5) |
                            ((value & 0x7f_0000_0000_0000L) >> 6) |
                            ((value & 0x7f00_0000_0000_0000L) >> 7);
                default:
                    throw new IllegalStateException();
            }
        }
    }

    public static class SafeVarintDecoder
            implements VarintDecoder
    {
        public long decodeVarint(long[] destination, byte[] array, int arrayLength)
        {
            int position = 0;
            int offset = 0;
            long result = 0;
            for (int shift = 0; offset < arrayLength; offset++) {
                result |= (long) (array[offset] & 0x7f) << shift;
                if ((array[offset] & 0x80) == 0) {
                    destination[position++] = result;
                    result = 0;
                    shift = 0;
                }
                else {
                    shift += 7;
                }
            }
            return position;
        }
    }

    interface VarintDecoder
    {
        long decodeVarint(long[] destination, byte[] data, int dataLength);
    }

    private static int getEncoded(long[] data, byte[] destination)
    {
        Random random = new Random(22);
        int offset = 0;
        for (int i = 0; i < COUNT; i++) {
            offset += encode(data[i], destination, offset);
        }
        return offset;
    }

    private static long[] getData()
    {
        int count = COUNT;
        Random random = new Random(22);
        long[] data = new long[count];
        for (int i = 0, offset = 0; i < count; i++) {
            data[i] = random.nextLong();
        }
        return data;
    }
    private static int encode(long value, byte[] destination, int offset)
    {
        int start = offset;
        while (offset < destination.length) {
            // if there are less than 7 bits left, we are done
            if ((value & ~0b111_1111) == 0) {
                destination[offset++] = (byte) value;
                return offset - start;
            }
            else {
                destination[offset++] = (byte) (0x80 | (value & 0x7f));
                value >>>= 7;
            }
        }
        // if buffer was not long enough make sure last value is a varint
        if (offset == destination.length) {
            destination[destination.length - 1] &= 0x7f;
        }
        return offset - start;
    }
    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkDecodeVarint.BenchmarkData();
        data.setup();
        data.useUnsafe = true;
        long countUnsafe = (long) new BenchmarkDecodeVarint().benchmarkDecodeVarints(data);
        assertTrue(Arrays.equals(data.destination, DATA));
        data = new BenchmarkDecodeVarint.BenchmarkData();
        data.setup();
        data.useUnsafe = false;
        long countSafe = (long) new BenchmarkDecodeVarint().benchmarkDecodeVarints(data);
        assertTrue(Arrays.equals(data.destination, DATA));
        assertEquals(countSafe, countUnsafe);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDecodeVarint.class.getSimpleName() + ".*")
                .jvmArgs("-Xmx5g")
                .build();
        new Runner(options).run();
    }
}
