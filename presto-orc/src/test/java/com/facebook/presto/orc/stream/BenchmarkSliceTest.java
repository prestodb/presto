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
import io.airlift.slice.ByteArrays;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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

import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSliceTest
{
    private static final long VARINT_MASK = 0x8080_8080_8080_8080L;
    private static final byte[] DATA = getData();

    @Benchmark
    @OperationsPerInvocation(10)
    public Object benchmarkBitCount(BenchmarkData data)
    {
        return data.bitCounter.countBits();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"false", "true"})
        boolean useUnsafe;

        BitCounter bitCounter;

        @Setup(Level.Invocation)
        public void setup()
        {
            if (useUnsafe) {
                bitCounter = new UnsafeBitCounter();
            }
            else {
                bitCounter = new SliceArrayBitCounter();
            }
            bitCounter.setData(DATA);
        }
    }

    public static class UnsafeBitCounter
            implements BitCounter
    {
        byte[] array;
        public int countBits()
        {
            int count = 0;
            int position = 0;
            while (position + SIZE_OF_LONG <= array.length) {
                long value = ByteArrays.getLong(array, position);
                count += Long.bitCount((value & VARINT_MASK) ^ VARINT_MASK);
                position += SIZE_OF_LONG;
            }
            while (position < array.length) {
                if ((array[position++] & 0x80) == 0) {
                    count++;
                }
            }
            return count;
        }

        public void setData(byte[] array) {
            this.array = array;
        }
    }

    public static class SliceArrayBitCounter
            implements BitCounter
    {
        BasicSliceInput input;
        byte[] buffer;

        public int countBits()
        {
            int count = 0;
            input.readFully(buffer);
            input.setPosition(0);
            int position = 0;
            while (position + SIZE_OF_LONG <= buffer.length) {
                long value = ByteArrays.getLong(buffer, position);
                count += Long.bitCount((value & VARINT_MASK) ^ VARINT_MASK);
                position += SIZE_OF_LONG;
            }
            while (position < buffer.length) {
                if ((buffer[position++] & 0x80) == 0) {
                    count++;
                }
            }
            return count;
        }
        public void setData(byte[] array) {
            Slice slice = Slices.wrappedBuffer(array);
            input = slice.getInput();
            buffer = new byte[array.length];
        }

    }
    public static class SliceBitCounter
            implements BitCounter
    {
        BasicSliceInput input;
        public int countBits()
        {
            int count = 0;
            while (input.position() + SIZE_OF_LONG <= input.length()) {
                long value = input.readLong();
                count += Long.bitCount((value & VARINT_MASK) ^ VARINT_MASK);
            }
            while (input.available() > 0) {
                if ((input.readByte() & 0x80) == 0) {
                    count++;
                }
            }
            return count;
        }

        public void setData(byte[] array) {
            Slice slice = Slices.wrappedBuffer(array);
            input = slice.getInput();
        }
    }

    public static class SafeBitCounter
            implements BitCounter
    {
        byte[] array;
        public int countBits()
        {
            int count = 0;
            int position = 0;
            while (position < array.length) {
                if ((array[position++] & 0x80) == 0) {
                    count++;
                }
            }
            return count;
        }

        public void setData(byte[] array) {
            this.array = array;
        }
    }

    interface BitCounter
    {
        int countBits();
        void setData(byte[] array);
    }
    private static byte[] getData()
    {
        int count = 1_000_000;
        Random random = new Random(22);
        byte[] data = new byte[count];
        random.nextBytes(data);
        return data;
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkSliceTest.BenchmarkData();
        data.setup();
        data.useUnsafe = true;
        Integer countUnsafe = (Integer) new BenchmarkSliceTest().benchmarkBitCount(data);
        data = new BenchmarkSliceTest.BenchmarkData();
        data.setup();
        data.useUnsafe = false;
        Integer countSafe = (Integer) new BenchmarkSliceTest().benchmarkBitCount(data);
        assertEquals(countSafe, countUnsafe);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSliceTest.class.getSimpleName() + ".*")
                .jvmArgs("-Xmx5g")
                .build();
        new Runner(options).run();
    }
}
