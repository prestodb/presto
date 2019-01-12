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
package io.prestosql.orc.stream;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.Slices;
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.prestosql.orc.stream.TestingBitPackingUtils.unpackGeneric;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@Fork(2)
@Warmup(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 5, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongBitPacker
{
    @Benchmark
    public Object baselineLength1(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 1, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(2)
    public Object baselineLength2(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 2, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(3)
    public Object baselineLength3(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 3, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(4)
    public Object baselineLength4(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 4, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(5)
    public Object baselineLength5(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 5, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(6)
    public Object baselineLength6(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 6, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(7)
    public Object baselineLength7(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 7, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(256)
    public Object baselineLength256(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        unpackGeneric(data.buffer, 0, 256, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    public Object optimizedLength1(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 1, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(2)
    public Object optimizedLength2(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 2, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(3)
    public Object optimizedLength3(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 3, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(4)
    public Object optimizedLength4(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 4, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(5)
    public Object optimizedLength5(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 5, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(6)
    public Object optimizedLength6(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 6, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(7)
    public Object optimizedLength7(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 7, data.bits, data.input);
        return data.buffer;
    }

    @Benchmark
    @OperationsPerInvocation(256)
    public Object optimizedLength256(BenchmarkData data)
            throws Throwable
    {
        data.input.setPosition(0);
        data.packer.unpack(data.buffer, 0, 256, data.bits, data.input);
        return data.buffer;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final long[] buffer = new long[256];
        private final LongBitPacker packer = new LongBitPacker();

        @Param({"1", "2", "4", "8", "16", "24", "32", "40", "48", "56", "64"})
        private int bits;

        private BasicSliceInput input;

        @Setup
        public void setup()
        {
            byte[] bytes = new byte[256 * 64];
            ThreadLocalRandom.current().nextBytes(bytes);
            input = Slices.wrappedBuffer(bytes).getInput();
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkLongBitPacker().baselineLength256(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkLongBitPacker.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
