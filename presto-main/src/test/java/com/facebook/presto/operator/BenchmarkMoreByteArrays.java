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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
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

import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.operator.MoreByteArrays.fill;
import static com.facebook.presto.operator.MoreByteArrays.setBytes;
import static com.facebook.presto.operator.MoreByteArrays.setInts;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMoreByteArrays
{
    @Benchmark
    public void fillToByteArray(BenchmarkData data)
    {
        fill(data.bytes, 0, data.bytes.length, (byte) 5);
    }

    @Benchmark
    public void fillToBasicSliceOutput(BenchmarkData data)
    {
        data.basicSliceOutput.reset();
        for (int i = 0; i < data.bytes.length; i++) {
            data.basicSliceOutput.writeByte((byte) 5);
        }
    }

    @Benchmark
    public void fillToDynamicSliceOutput(BenchmarkData data)
    {
        data.dynamicSliceOutput.reset();
        for (int i = 0; i < data.bytes.length; i++) {
            data.dynamicSliceOutput.writeByte((byte) 5);
        }
    }

    @Benchmark
    public void setBytesToByteArray(BenchmarkData data)
    {
        setBytes(data.bytes, 0, data.byteValues, 0, data.bytes.length);
    }

    @Benchmark
    public void setBytesToBasicSliceOutput(BenchmarkData data)
    {
        data.basicSliceOutput.reset();
        data.basicSliceOutput.writeBytes(data.slice, 0, data.slice.length());
    }

    @Benchmark
    public void setBytesToDynamicSliceOutput(BenchmarkData data)
    {
        data.dynamicSliceOutput.reset();
        data.dynamicSliceOutput.writeBytes(data.slice, 0, data.slice.length());
    }

    @Benchmark
    public void setIntsToByteArray(BenchmarkData data)
    {
        setInts(data.bytes, 0, data.intValues, 0, data.POSITIONS_PER_PAGE);
    }

    @Benchmark
    public void setIntsToBasicSliceOutput(BenchmarkData data)
    {
        data.basicSliceOutput.reset();
        for (int i = 0; i < data.POSITIONS_PER_PAGE; i++) {
            data.basicSliceOutput.writeInt(data.intValues[i]);
        }
    }

    @Benchmark
    public void setIntsToDynamicSliceOutput(BenchmarkData data)
    {
        data.dynamicSliceOutput.reset();
        for (int i = 0; i < data.POSITIONS_PER_PAGE; i++) {
            data.dynamicSliceOutput.writeInt(data.intValues[i]);
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITIONS_PER_PAGE = 10000;

        private final Random random = new Random(0);

        private final int[] intValues = IntStream.range(0, POSITIONS_PER_PAGE).map(i -> random.nextInt()).toArray();
        private final byte[] byteValues = new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];
        private final Slice slice = Slices.wrappedBuffer(byteValues);

        private final byte[] bytes = new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];
        private final SliceOutput basicSliceOutput = Slices.wrappedBuffer(new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE]).getOutput();
        private final SliceOutput dynamicSliceOutput = new DynamicSliceOutput(POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE);

        @Setup
        public void setup()
        {
            random.nextBytes(byteValues);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkMoreByteArrays.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
