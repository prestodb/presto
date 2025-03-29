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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.common.block.Block;
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

import static com.facebook.presto.block.BlockAssertions.createRandomDictionaryBlock;
import static com.facebook.presto.operator.UncheckedByteArrays.setLongUnchecked;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(0)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkReadBlock
{
    @Benchmark
    public int sequentialCopyLongValues(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.longValues[i]);
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyLongArrayBlock(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.blockNoNulls.getLong(i));
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyUncheckedLongArrayBlock(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.blockNoNulls.getLongUnchecked(i));
        }
        return index;
    }

    @Benchmark
    public int randomCopyLongValues(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.longValues[data.positions[i]]);
        }
        return index;
    }

    @Benchmark
    public int randomCopyLongArrayBlock(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.blockNoNulls.getLong(data.positions[i]));
        }
        return index;
    }

    @Benchmark
    public int randomCopyUncheckedLongArrayBlock(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.blockNoNulls.getLongUnchecked(data.positions[i]));
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyLongValuesWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.longValues[i]);
            if (!data.nulls[i]) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyLongArrayBlockWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.blockWithNulls.getLong(i));
            if (!data.blockWithNulls.isNull(i)) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int sequentialCopyUncheckedLongArrayBlockWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.blockWithNulls.getLongUnchecked(i));
            if (!data.blockWithNulls.isNullUnchecked(i)) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int randomCopyLongValuesWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.longValues[data.positions[i]]);
            if (!data.nulls[data.positions[i]]) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int randomCopyLongArrayBlockWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.blockNoNulls.getLong(data.positions[i]));
            if (!data.blockWithNulls.isNull(data.positions[i])) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int randomCopyUncheckedLongArrayBlockWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.blockNoNulls.getLongUnchecked(data.positions[i]));
            if (!data.blockWithNulls.isNullUnchecked(data.positions[i])) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int randomCopyLongValuesWithDictionary(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.longValues[data.ids[data.positions[i]]]);
        }
        return index;
    }

    @Benchmark
    public int randomCopyDictionaryBlock(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.dictionaryBlockNoNulls.getLong(data.positions[i]));
        }
        return index;
    }

    @Benchmark
    public int randomCopyUncheckedDictionaryBlock(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            index = setLongUnchecked(data.bytes, index, data.dictionaryBlockNoNulls.getLongUnchecked(data.positions[i]));
        }
        return index;
    }

    @Benchmark
    public int randomCopyLongValuesWithDictionaryWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.longValues[data.positions[i]]);
            if (!data.nulls[data.ids[data.positions[i]]]) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int randomCopyDictionaryBlockWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.dictionaryBlockWithNulls.getLong(data.positions[i]));
            if (!data.dictionaryBlockWithNulls.isNull(data.positions[i])) {
                index = newIndex;
            }
        }
        return index;
    }

    @Benchmark
    public int randomCopyUncheckedDictionaryBlockWithNulls(BenchmarkData data)
    {
        int index = 0;
        for (int i = 0; i < data.longValues.length; i++) {
            int newIndex = setLongUnchecked(data.bytes, index, data.dictionaryBlockWithNulls.getLongUnchecked(data.positions[i]));
            if (!data.dictionaryBlockWithNulls.isNullUnchecked(data.positions[i])) {
                index = newIndex;
            }
        }
        return index;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int POSITIONS_PER_PAGE = 10000;

        private final Random random = new Random(0);

        private final long[] longValues = new long[POSITIONS_PER_PAGE];
        private final boolean[] nulls = new boolean[POSITIONS_PER_PAGE];
        private final int[] ids = new int[POSITIONS_PER_PAGE];
        private final int[] positions = new int[POSITIONS_PER_PAGE];

        private final Block blockNoNulls = BlockAssertions.createRandomLongsBlock(POSITIONS_PER_PAGE, 0.0f);
        private final Block blockWithNulls = BlockAssertions.createRandomLongsBlock(POSITIONS_PER_PAGE, 0.2f);
        private final Block dictionaryBlockNoNulls = createRandomDictionaryBlock(blockNoNulls, POSITIONS_PER_PAGE);
        private final Block dictionaryBlockWithNulls = createRandomDictionaryBlock(blockWithNulls, POSITIONS_PER_PAGE);

        private final byte[] bytes = new byte[POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];

        @Setup
        public void setup()
        {
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                longValues[i] = random.nextLong();
                ids[i] = random.nextInt(POSITIONS_PER_PAGE / 10);
                positions[i] = i;
                nulls[i] = i % 7 == 0;
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkReadBlock.class.getSimpleName() + ".*")
                .build();
        new Runner(options).run();
    }
}
