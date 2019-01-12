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
package io.prestosql.array;

import io.airlift.slice.Slice;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static io.airlift.slice.Slices.wrappedIntArray;
import static io.airlift.slice.Slices.wrappedLongArray;

@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(4)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
public class BenchmarkReferenceCountMap
{
    private static final int NUMBER_OF_ENTRIES = 1_000_000;
    private static final int NUMBER_OF_BASES = 100;

    @State(Scope.Thread)
    public static class Data
    {
        @Param({"int", "double", "long", "byte"})
        private String arrayType = "int";
        private Object[] bases = new Object[NUMBER_OF_BASES];
        private Slice[] slices = new Slice[NUMBER_OF_ENTRIES];

        @Setup
        public void setup()
        {
            for (int i = 0; i < NUMBER_OF_BASES; i++) {
                switch (arrayType) {
                    case "int":
                        bases[i] = new int[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                        break;
                    case "double":
                        bases[i] = new double[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                        break;
                    case "long":
                        bases[i] = new long[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                        break;
                    case "byte":
                        bases[i] = new byte[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }

            for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
                Object base = bases[ThreadLocalRandom.current().nextInt(NUMBER_OF_BASES)];
                switch (arrayType) {
                    case "int":
                        int[] intBase = (int[]) base;
                        slices[i] = wrappedIntArray(intBase, 0, intBase.length);
                        break;
                    case "double":
                        double[] doubleBase = (double[]) base;
                        slices[i] = wrappedDoubleArray(doubleBase, 0, doubleBase.length);
                        break;
                    case "long":
                        long[] longBase = (long[]) base;
                        slices[i] = wrappedLongArray(longBase, 0, longBase.length);
                        break;
                    case "byte":
                        byte[] byteBase = (byte[]) base;
                        slices[i] = wrappedBuffer(byteBase, 0, byteBase.length);
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(NUMBER_OF_ENTRIES)
    public ReferenceCountMap benchmarkInserts(Data data)
    {
        ReferenceCountMap map = new ReferenceCountMap();
        for (int i = 0; i < NUMBER_OF_ENTRIES; i++) {
            map.incrementAndGet(data.slices[i]);
            map.incrementAndGet(data.slices[i].getBase());
        }
        return map;
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .include(".*" + BenchmarkReferenceCountMap.class.getSimpleName() + ".*")
                .addProfiler(GCProfiler.class)
                .jvmArgs("-XX:+UseG1GC")
                .build();

        new Runner(options).run();
    }
}
