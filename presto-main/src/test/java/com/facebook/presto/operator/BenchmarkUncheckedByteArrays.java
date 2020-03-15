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
package com.facebook.presto.operator;

import com.google.common.primitives.Booleans;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static com.facebook.presto.operator.UncheckedByteArrays.setLongUnchecked;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static sun.misc.Unsafe.ARRAY_BYTE_BASE_OFFSET;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkUncheckedByteArrays
{
    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkUncheckedByteArrays.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }

    @Benchmark
    public int sequentialCopyToByteArray(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.longValues[i]);
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyToBasicSliceOutput(BenchmarkData data)
    {
        return sequentialCopyToSliceOutput(data.basicSliceOutput, data.longValues);
    }

    @Benchmark
    public int sequentialCopyToDynamicSliceOutput(BenchmarkData data)
    {
        return sequentialCopyToSliceOutput(data.dynamicSliceOutput, data.longValues);
    }

    @Benchmark
    public int sequentialCopyToTestingSliceOutput(BenchmarkData data)
    {
        TestingSliceOutput sliceOutput = data.testingSliceOutput;
        sliceOutput.reset();
        for (int i = 0; i < data.longValues.length; i++) {
            sliceOutput.writeLong(data.longValues[i]);
        }
        return sliceOutput.size();
    }

    @Benchmark
    public int sequentialCopyWithNullsToByteArray(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.longValues[i]);
            // Only update the index when it's not null. This will be compiled to conditional move instruction instead of compare and jumps.
            if (data.nulls[i]) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyWithNullsToByteArrayNaive(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            // This will be compiled to compare and jumps.
            if (!data.nulls[i]) {
                index = setLongUnchecked(data.bytes, index, data.longValues[i]);
            }
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyWithNullsToBasicSliceOutput(BenchmarkData data)
    {
        return sequentialCopyWithNullsToSliceOutput(data.basicSliceOutput, data.longValues, data.nulls);
    }

    @Benchmark
    public int sequentialCopyWithNullsToDynamicSliceOutput(BenchmarkData data)
    {
        return sequentialCopyWithNullsToSliceOutput(data.dynamicSliceOutput, data.longValues, data.nulls);
    }

    @Benchmark
    public int sequentialCopyWithNullsToTestingSliceOutputNaive(BenchmarkData data)
    {
        TestingSliceOutput sliceOutput = data.testingSliceOutput;
        sliceOutput.reset();
        for (int i = 0; i < data.longValues.length; i++) {
            if (data.nulls[i]) {
                sliceOutput.writeLong(data.longValues[i]);
            }
        }
        return sliceOutput.size();
    }

    @Benchmark
    public int sequentialCopyWithNullsToTestingSliceOutput(BenchmarkData data)
    {
        TestingSliceOutput sliceOutput = data.testingSliceOutput;
        sliceOutput.reset();
        for (int i = 0; i < data.longValues.length; i++) {
            sliceOutput.writeLong(data.longValues[i], data.nulls[i]);
        }
        return sliceOutput.size();
    }

    @Benchmark
    public int randomCopyToByteArray(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, data.positions[i] * ARRAY_LONG_INDEX_SCALE, data.longValues[i]);
        }
        return index;
    }

    // We can't apply the trick on null check when copying to random locations in the byte array
    @Benchmark
    public int randomCopyWithNullsToByteArray(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            if (data.nulls[i]) {
                index = setLongUnchecked(data.bytes, data.positions[i] * ARRAY_LONG_INDEX_SCALE, data.longValues[i]);
            }
        }
        return index;
    }

    private int sequentialCopyToSliceOutput(SliceOutput sliceOutput, long[] values)
    {
        sliceOutput.reset();
        for (int i = 0; i < values.length; i++) {
            sliceOutput.writeLong(values[i]);
        }
        return sliceOutput.size();
    }

    private int sequentialCopyWithNullsToSliceOutput(SliceOutput sliceOutput, long[] values, boolean[] nulls)
    {
        sliceOutput.reset();
        for (int i = 0; i < values.length; i++) {
            if (!nulls[i]) {
                sliceOutput.writeLong(values[i]);
            }
        }
        return sliceOutput.size();
    }

    private static class TestingSliceOutput
    {
        private static Unsafe unsafe;
        public int size;
        private byte[] buffer;

        TestingSliceOutput(int initialSize)
        {
            buffer = new byte[initialSize];
        }

        public void writeLong(long value)
        {
            unsafe.putLong(buffer, (long) size + ARRAY_BYTE_BASE_OFFSET, value);
            size += ARRAY_LONG_INDEX_SCALE;
        }

        public void writeLong(long value, int address)
        {
            unsafe.putLong(buffer, (long) address + ARRAY_BYTE_BASE_OFFSET, value);
        }

        public void writeLong(long value, boolean isNull)
        {
            unsafe.putLong(buffer, (long) size + ARRAY_BYTE_BASE_OFFSET, value);
            if (!isNull) {
                size += ARRAY_LONG_INDEX_SCALE;
            }
        }

        public int size()
        {
            return size;
        }

        public void reset()
        {
            size = 0;
        }

        static {
            try {
                Field field = Unsafe.class.getDeclaredField("theUnsafe");
                field.setAccessible(true);
                unsafe = (Unsafe) field.get(null);
                if (unsafe == null) {
                    throw new RuntimeException("Unsafe access not available");
                }
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITIONS_PER_PAGE = 10000;

        private final Random random = new Random(0);

        private final long[] longValues = LongStream.range(0, POSITIONS_PER_PAGE).map(i -> random.nextLong()).toArray();
        private final boolean[] nulls = Booleans.toArray(Stream.generate(() -> random.nextBoolean()).limit(POSITIONS_PER_PAGE).collect(Collectors.toCollection(ArrayList::new)));
        private final int[] positions = IntStream.range(0, POSITIONS_PER_PAGE).toArray();

        private final byte[] byteValues = new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];

        private final byte[] bytes = new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];
        private final SliceOutput basicSliceOutput = Slices.wrappedBuffer(new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE]).getOutput();
        private final SliceOutput dynamicSliceOutput = new DynamicSliceOutput(POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE);
        private final TestingSliceOutput testingSliceOutput = new TestingSliceOutput(POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE);

        @Setup
        public void setup()
        {
            random.nextBytes(byteValues);
        }
    }
}
