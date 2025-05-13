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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockFlattener;
import com.facebook.presto.common.block.BlockLease;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.IntArrayBlock;
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
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.openjdk.jmh.annotations.Scope.Thread;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBlockFlattener
{
    private static class ThrowawayArrayAllocator
            implements ArrayAllocator
    {
        @Override
        public int[] borrowIntArray(int positionCount)
        {
            return new int[positionCount];
        }

        @Override
        public void returnArray(int[] array)
        {
            // no op
        }

        @Override
        public byte[] borrowByteArray(int positionCount)
        {
            return new byte[positionCount];
        }

        @Override
        public void returnArray(byte[] array)
        {
            // no op
        }

        @Override
        public int getBorrowedArrayCount()
        {
            return 0;
        }

        @Override
        public long getEstimatedSizeInBytes()
        {
            return 0;
        }
    }

    @State(Thread)
    public static class BaseContext
    {
        protected Block block;

        @Param({"1000", "10000", "100000", "1000000"})
        protected int blockSize;

        @Param({"1", "2", "3", "4", "5"})
        protected int nestedLevel;

        @Param({"1", "10", "100", "1000"})
        protected int numberOfIterations;

        @Setup
        public void setUp()
        {
            Random r = ThreadLocalRandom.current();
            int[] data = new int[blockSize];

            for (int i = 0; i < blockSize; i++) {
                data[i] = r.nextInt(blockSize);
            }

            block = new IntArrayBlock(blockSize, Optional.empty(), data);
            for (int i = 1; i < nestedLevel; i++) {
                int[] ids = IntStream.range(0, blockSize).toArray();
                Collections.shuffle(Arrays.asList(ids));
                block = new DictionaryBlock(block, ids);
            }
        }
    }

    @State(Thread)
    public static class FlattenContext
            extends BaseContext
    {
        private BlockFlattener flattener;

        @Param({"false", "true"})
        private boolean reuseArrays;

        @Setup
        public void setUp()
        {
            super.setUp();

            if (reuseArrays) {
                flattener = new BlockFlattener(new SimpleArrayAllocator());
            }
            else {
                flattener = new BlockFlattener(new ThrowawayArrayAllocator());
            }
        }
    }

    @Benchmark
    public long benchmarkWithFlatten(FlattenContext context)
    {
        long sum = 0;
        for (int i = 0; i < context.numberOfIterations; i++) {
            try (BlockLease lease = context.flattener.flatten(context.block)) {
                Block block = lease.get();

                for (int j = 0; j < context.blockSize; j++) {
                    sum += block.getInt(j);
                }
            }
        }
        return sum;
    }

    @Benchmark
    public long benchmarkWithoutFlatten(BaseContext context)
    {
        int sum = 0;
        Block block = context.block;

        for (int i = 0; i < context.numberOfIterations; i++) {
            for (int j = 0; j < context.blockSize; j++) {
                sum += block.getInt(j);
            }
        }

        return sum;
    }

    @Test
    public void testBenchmarkWithFlatten()
    {
        FlattenContext context = new FlattenContext();
        context.setUp();
        benchmarkWithFlatten(context);
    }

    @Test
    public void testBenchmarkWithoutFlatten()
    {
        BaseContext context = new BaseContext();
        context.setUp();
        benchmarkWithoutFlatten(context);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBlockFlattener.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
