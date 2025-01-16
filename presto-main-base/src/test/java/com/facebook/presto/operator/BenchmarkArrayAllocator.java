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

import com.facebook.presto.common.block.ArrayAllocator;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Random;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkArrayAllocator
{
    @Benchmark
    public void borrowAndReturnArrays(BenchmarkData data)
    {
        for (int i = 0; i < 100000; i++) {
            data.borrowAndReturnArrays();
        }
    }

    @Test
    public void verifyBorrowAndReturnArrays()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        borrowAndReturnArrays(data);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int ARRAY_COUNT = 1000;

        private final Random random = new Random(0);
        private final int[][] allocatedArrays = new int[ARRAY_COUNT][];
        private final int[] arrayLengths = new int[ARRAY_COUNT];

        @Param({"SimpleArrayAllocator", "UncheckedStackArrayAllocator"})
        private String arrayAllocatorType = "SimpleArrayAllocator";
        private ArrayAllocator arrayAllocator;

        @Setup
        public void setup()
        {
            switch (arrayAllocatorType) {
                case "SimpleArrayAllocator":
                    arrayAllocator = new SimpleArrayAllocator();
                    break;
                case "UncheckedStackArrayAllocator":
                    arrayAllocator = new UncheckedStackArrayAllocator();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported arrayAllocatorType");
            }

            for (int i = 0; i < ARRAY_COUNT; i++) {
                arrayLengths[i] = random.nextInt(100);
            }
        }

        private void borrowAndReturnArrays()
        {
            for (int i = 0; i < ARRAY_COUNT; i++) {
                allocatedArrays[i] = arrayAllocator.borrowIntArray(arrayLengths[i]);
            }

            for (int i = ARRAY_COUNT - 1; i >= 0; i--) {
                arrayAllocator.returnArray(allocatedArrays[i]);
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkArrayAllocator.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
