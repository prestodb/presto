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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import io.airlift.slice.BasicSliceOutput;
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
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.LinuxPerfAsmProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static sun.misc.Unsafe.ARRAY_LONG_INDEX_SCALE;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPartitionedOutputOperatorAppendToBuffers
{
    private static final int RUNS = 1000;

    @Benchmark
    public void copyLongValuesToByteArrayBufferChannelFirst(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.longValues[pageIndex][channelIndex];
                    boolean[] nulls = data.nulls[pageIndex][channelIndex];
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongsWithNulls(
                                    data.longValuesBuffers[partition][channelIndex], longValueBufferIndexes[partition][channelIndex], longValues,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongs(
                                    data.longValuesBuffers[partition][channelIndex], longValueBufferIndexes[partition][channelIndex], longValues,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongValuesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        long[] longValues = data.longValues[pageIndex][channelIndex];
                        boolean[] nulls = data.nulls[pageIndex][channelIndex];
                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongsWithNulls(
                                    data.longValuesBuffers[partition][channelIndex], longValueBufferIndexes[partition][channelIndex], longValues,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongs(
                                    data.longValuesBuffers[partition][channelIndex], longValueBufferIndexes[partition][channelIndex], longValues,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongPagesToByteArrayBufferChannelFirst(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.bigintDataPages[pageIndex];
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    Block bigintBlock = page.getBlock(channelIndex);
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        int[] positions = data.positions[partition];
                        int positionCount = data.positionCounts[partition];

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putBigintWithNullsBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putBigintBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongPagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.bigintDataPages[pageIndex];
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int[] positions = data.positions[partition];
                    int positionCount = data.positionCounts[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        Block bigintBlock = page.getBlock(channelIndex);

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putBigintWithNullsBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putBigintBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyDictionaryLongValuesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int positionCounts = data.positionCounts[partition];
                    int[] positions = data.positions[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        long[] longValuesForDicitionary = data.longValuesForDicitionary[pageIndex][channelIndex];
                        int[] ids  = data.ids[pageIndex][channelIndex];
                        boolean[] nulls = data.nulls[pageIndex][channelIndex];
                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongsWithNulls(
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex],
                                    longValuesForDicitionary,
                                    ids,
                                    nulls,
                                    positions,
                                    0,
                                    positionCounts,
                                    0);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongs(
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex],
                                    longValuesForDicitionary,
                                    ids,
                                    positions,
                                    0,
                                    positionCounts,
                                    0);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyDictionaryBigintPagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.dictionaryDataPages[pageIndex];
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int[] positions = data.positions[partition];
                    int positionCount = data.positionCounts[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        Block dictionaryBigintBlock = page.getBlock(channelIndex);

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putBigintWithNullsBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putBigintBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyUncheckedDictionaryBigintPagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.dictionaryDataPages[pageIndex];
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int[] positions = data.positions[partition];
                    int positionCount = data.positionCounts[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        Block dictionaryBigintBlock = page.getBlock(channelIndex);

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintWithNullsBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyCompositeLongValuesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        long[] longValues = data.longValues[pageIndex][channelIndex];
                        long[] longValuesForDicitionary = data.longValuesForDicitionary[pageIndex][channelIndex];
                        int[] ids  = data.ids[pageIndex][channelIndex];
                        boolean[] nulls = data.nulls[pageIndex][channelIndex];
                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongsWithNulls(
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex],
                                    longValues,
                                    ids,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongsWithNulls(
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex],
                                    longValuesForDicitionary,
                                    ids,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongs(
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex],
                                    longValues,
                                    ids,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                            longValueBufferIndexes[partition][channelIndex] = ByteArrayUtils.writeLongs(
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex],
                                    longValuesForDicitionary,
                                    ids,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyCompositePagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.compositeDataPages[pageIndex];
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int[] positions = data.positions[partition];
                    int positionCount = data.positionCounts[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        Block bigintBlock = page.getBlock(channelIndex * 2);
                        Block dictionaryBigintBlock = page.getBlock(channelIndex * 2 + 1);

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putBigintWithNullsBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                            longValueBufferIndexes[partition][channelIndex] = putBigintWithNullsBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putBigintBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                            longValueBufferIndexes[partition][channelIndex] = putBigintBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyCompositeUncheckedPagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data .compositeDataPages[pageIndex];
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int[] positions = data.positions[partition];
                    int positionCount = data.positionCounts[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        Block bigintBlock = page.getBlock(channelIndex * 2);
                        Block dictionaryBigintBlock = page.getBlock(channelIndex * 2 + 1);

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintWithNullsBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintWithNullsBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintBlockToByteArrayBuffer(
                                    dictionaryBigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongValuesToDynamicSliceOutput(BenchmarkData data)
    {
        copyLongValuesToSliceOutput(data, data::getDynamicSliceOutput);
    }

    @Benchmark
    public void copyLongValuesToBasicSliceOutput(BenchmarkData data)
    {
        copyLongValuesToSliceOutput(data, data::getBasicSliceOutput);
    }

    private void copyLongValuesToSliceOutput(BenchmarkData data, BiFunction<Integer, Integer, SliceOutput> getSliceOutput)
    {
        for (int i = 0; i < RUNS; i++) {
            data.resetSliceOutput();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.longValues[pageIndex][channelIndex];
                    boolean[] nulls = data.nulls[pageIndex][channelIndex];
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        SliceOutput sliceOutput = getSliceOutput.apply(partition, channelIndex);
                        if (data.mayHaveNull) {
                            putLongValuesWithNullsToSliceOutput(
                                    longValues,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                        else {
                            putLongValuesToSliceOutput(
                                    longValues,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongValuesToTestingSliceOutputEncapsulated(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            data.resetTestingSliceOutput();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.longValues[pageIndex][channelIndex];
                    boolean[] nulls = data.nulls[pageIndex][channelIndex];
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        TestingSliceOutput sliceOutput = data.testingSliceOutput[partition][channelIndex];
                        if (data.mayHaveNull) {
                            putLongValuesWithNullsToTestingSliceOutputEncapsulated(
                                    longValues,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                        else {
                            putLongValuesToTestingSliceOutput(
                                    longValues,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongValuesToTestingSliceOutputWriteLongArray(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            data.resetTestingSliceOutput();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.longValues[pageIndex][channelIndex];
                    boolean[] nulls = data.nulls[pageIndex][channelIndex];
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        TestingSliceOutput sliceOutput = data.testingSliceOutput[partition][channelIndex];
                        if (data.mayHaveNull) {
                            putLongValuesWithNullsToTestingSliceOutputWriteLongArray(
                                    longValues,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                        else {
                            putLongValuesToTestingSliceOutput(
                                    longValues,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongValuesToTestingSliceOutputNoEncapsulation(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            data.resetTestingSliceOutput();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.longValues[pageIndex][channelIndex];
                    boolean[] nulls = data.nulls[pageIndex][channelIndex];
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        TestingSliceOutput sliceOutput = data.testingSliceOutput[partition][channelIndex];
                        if (data.mayHaveNull) {
                            putLongValuesWithNullsToTestingSliceOutputNoEncapsulation(
                                    longValues,
                                    nulls,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                        else {
                            putLongValuesToTestingSliceOutput(
                                    longValues,
                                    data.positions[partition],
                                    0,
                                    data.positionCounts[partition],
                                    0,
                                    sliceOutput);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongPagesToDynamicSliceOutput(BenchmarkData data)
    {
        copyLongPagesToSliceOutput(data, data::getDynamicSliceOutput);
    }

    @Benchmark
    public void copyLongPagesToBasicSliceOutput(BenchmarkData data)
    {
        copyLongPagesToSliceOutput(data, data::getBasicSliceOutput);
    }

    private void copyLongPagesToSliceOutput(BenchmarkData data, BiFunction<Integer, Integer, SliceOutput> getSliceOutput)
    {
        for (int i = 0; i < RUNS; i++) {
            data.resetSliceOutput();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.bigintDataPages[pageIndex];
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    Block bigintBlock = page.getBlock(channelIndex);
                    for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                        int[] positions = data.positions[partition];
                        int positionCount = data.positionCounts[partition];
                        SliceOutput sliceOutput = getSliceOutput.apply(partition, channelIndex);

                        if (data.mayHaveNull) {
                            putBigintWithNullsBlockToSliceOutput(bigintBlock, positions, positionCount, sliceOutput);
                        }
                        else {
                            putBigintBlockToSliceOutput(bigintBlock, positions, positionCount, sliceOutput);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void copyLongPagesUncheckedToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.bigintDataPages[pageIndex];
                for (int partition = 0; partition < data.PARTITION_COUNT; partition++) {
                    int[] positions = data.positions[partition];
                    int positionCount = data.positionCounts[partition];

                    for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                        Block bigintBlock = page.getBlock(channelIndex);

                        if (data.mayHaveNull) {
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintWithNullsBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                        else {
                            longValueBufferIndexes[partition][channelIndex] = putUncheckedBigintBlockToByteArrayBuffer(
                                    bigintBlock,
                                    positions,
                                    positionCount,
                                    data.longValuesBuffers[partition][channelIndex],
                                    longValueBufferIndexes[partition][channelIndex]);
                        }
                    }
                }
            }
        }
    }

    @Benchmark
    public void sequentialCopyLongValuesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();
            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    long[] longValues = data.longValues[pageIndex][channelIndex];
                    boolean[] nulls = data.nulls[pageIndex][channelIndex];
                    if (data.mayHaveNull) {
                        ByteArrayUtils.writeLongsWithNulls(
                                data.longValuesBuffers[0][channelIndex],
                                longValueBufferIndexes[0][channelIndex],
                                longValues,
                                nulls,
                                0,
                                longValues.length,
                                0);
                    }
                    else {
                        ByteArrayUtils.writeLongs(
                                data.longValuesBuffers[0][channelIndex],
                                longValueBufferIndexes[0][channelIndex],
                                longValues,
                                0,
                                longValues.length,
                                0);
                    }
                }
            }
        }
    }

    @Benchmark
    public void sequentialCopyLongPagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.bigintDataPages[pageIndex];
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    Block bigintBlock = page.getBlock(channelIndex);

                    if (data.mayHaveNull) {
                        putAllBigintWithNullsBlockDataToByteArrayBuffer(
                                bigintBlock,
                                data.longValuesBuffers[0][channelIndex],
                                longValueBufferIndexes[0][channelIndex]);
                    }
                    else {
                        putAllBigintBlockDataToByteArrayBuffer(
                                bigintBlock,
                                data.longValuesBuffers[0][channelIndex],
                                longValueBufferIndexes[0][channelIndex]);
                    }
                }
            }
        }
    }

    @Benchmark
    public void sequentialCopyUncheckedLongPagesToByteArrayBuffer(BenchmarkData data)
    {
        for (int i = 0; i < RUNS; i++) {
            int[][] longValueBufferIndexes = data.getLongValueBufferIndexes();

            int pageCount = data.PAGE_COUNT;
            for (int pageIndex = 0; pageIndex < pageCount; pageIndex++) {
                Page page = data.bigintDataPages[pageIndex];
                for (int channelIndex = 0; channelIndex < data.channelCount; channelIndex++) {
                    Block bigintBlock = page.getBlock(channelIndex);

                    if (data.mayHaveNull) {
                        putAllUncheckedBigintWithNullsBlockDataToByteArrayBuffer(
                                bigintBlock,
                                data.longValuesBuffers[0][channelIndex],
                                longValueBufferIndexes[0][channelIndex]);
                    }
                    else {
                        putAllUncheckedBigintBlockDataToByteArrayBuffer(
                                bigintBlock,
                                data.longValuesBuffers[0][channelIndex],
                                longValueBufferIndexes[0][channelIndex]);
                    }
                }
            }
        }
    }

    private static int putBigintWithNullsBlockToByteArrayBuffer(Block block, int[] positions, int positionCount, byte[] longValueBuffer, int longBufferIndex)
    {
        for (int j = 0; j < positionCount; j++) {
            int position = positions[j];
            long longValue = block.getLong(positions[j]);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            if (!block.isNull(position)) {
                longBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }

        return longBufferIndex;
    }

    private static int putBigintBlockToByteArrayBuffer(Block block, int[] positions, int positionCount, byte[] longValueBuffer, int longBufferIndex)
    {
        for (int j = 0; j < positionCount; j++) {
            long longValue = block.getLong(positions[j]);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            longBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }

        return longBufferIndex;
    }

    private static int putUncheckedBigintWithNullsBlockToByteArrayBuffer(Block block, int[] positions, int positionCount, byte[] longValueBuffer, int longBufferIndex)
    {
        int offsetBase = block.getOffsetBase();
        for (int j = 0; j < positionCount; j++) {
            int position = positions[j];
            //long longValue = block.getLongUnchecked(positions[j]);
            long longValue = block.getLongUnchecked(positions[j] + offsetBase);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            if (!block.isNullUnchecked(position + offsetBase)) {
            //if (!block.isNullUnchecked(position)) {
                longBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }

        return longBufferIndex;
    }

    private static int putUncheckedBigintBlockToByteArrayBuffer(Block block, int[] positions, int positionCount, byte[] longValueBuffer, int longBufferIndex)
    {
        int offsetBase = block.getOffsetBase();
        for (int j = 0; j < positionCount; j++) {
            long longValue = block.getLongUnchecked(positions[j] + offsetBase);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            longBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }

        return longBufferIndex;
    }

    private static int putLongValuesWithNullsToSliceOutput(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, SliceOutput sliceOutput)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i] + offsetBase;
            long value = values[positions[i] + offsetBase];
            if (!nulls[position]) {
                sliceOutput.writeLong(value);
            }
        }
        return sliceOutput.size();
    }

    private static int putLongValuesToSliceOutput(long[] values, int[] positions, int positionsOffset, int batchSize, int offsetBase, SliceOutput sliceOutput)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            sliceOutput.writeLong(values[positions[i] + offsetBase]);
        }
        return sliceOutput.size();
    }

    private static int putBigintWithNullsBlockToSliceOutput(Block block, int[] positions, int positionCount, SliceOutput sliceOutput)
    {
        for (int j = 0; j < positionCount; j++) {
            int position = positions[j];
            long value = block.getLong(position);
            sliceOutput.writeLong(value);
            if (!block.isNull(position)) {
                sliceOutput.writeLong(value);
            }
        }

        return sliceOutput.size();
    }

    private static int putBigintBlockToSliceOutput(Block block, int[] positions, int positionCount, SliceOutput sliceOutput)
    {
        for (int j = 0; j < positionCount; j++) {
            sliceOutput.writeLong(block.getLong(positions[j]));
        }

        return sliceOutput.size();
    }

    private static int putLongValuesToTestingSliceOutput(long[] values, int[] positions, int positionsOffset, int batchSize, int offsetBase, TestingSliceOutput sliceOutput)
    {
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            sliceOutput.writeLong(values[positions[i] + offsetBase]);
        }
        return sliceOutput.size();
    }

    private static int putLongValuesWithNullsToTestingSliceOutputNoEncapsulation(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, TestingSliceOutput sliceOutput)
    {
        if (sliceOutput.capacity() < batchSize * 8) {
            throw new IllegalArgumentException(format("batchSize %d is too large to fit in buffer %d", batchSize, sliceOutput.capacity()));
        }

        int address = 0;
        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            long value = values[positions[i] + offsetBase];
            sliceOutput.writeLong(values[position], address);
            if (!nulls[position]) {
                address += ARRAY_LONG_INDEX_SCALE;
            }
        }
        return sliceOutput.size();
    }

    private static void putLongValuesWithNullsToTestingSliceOutputEncapsulated(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, TestingSliceOutput sliceOutput)
    {
        if (sliceOutput.capacity() < batchSize * 8) {
            throw new IllegalArgumentException(format("batchSize %d is too large to fit in buffer %d", batchSize, sliceOutput.capacity()));
        }

        for (int i = positionsOffset; i < positionsOffset + batchSize; i++) {
            int position = positions[i];
            long value = values[position + offsetBase];
            boolean isNull = nulls[position + offsetBase];
            sliceOutput.writeLong(value, isNull);
        }
    }

    private static void putLongValuesWithNullsToTestingSliceOutputWriteLongArray(long[] values, boolean[] nulls, int[] positions, int positionsOffset, int batchSize, int offsetBase, TestingSliceOutput sliceOutput)
    {
        if (sliceOutput.capacity() < batchSize * 8) {
            throw new IllegalArgumentException(format("batchSize %d is too large to fit in buffer %d", batchSize, sliceOutput.capacity()));
        }

        // Use values and nulls as temp array to store compacted data
        for (int i = 0; i < batchSize; i++) {
            int position = positions[i + positionsOffset] + offsetBase;
            values[i] = values[position];
            nulls[i] = nulls[position];
        }

        sliceOutput.writeLongArray(values, nulls, 0, batchSize);
    }

    private static int putAllBigintBlockDataToByteArrayBuffer(Block block, byte[] longValueBuffer, int longBufferIndex)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            long longValue = block.getLong(position);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            longBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }

        return longBufferIndex;
    }

    private static int putAllBigintWithNullsBlockDataToByteArrayBuffer(Block block, byte[] longValueBuffer, int longBufferIndex)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            long longValue = block.getLong(position);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            if (!block.isNull(position)) {
                longBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }

        return longBufferIndex;
    }

    private static int putAllUncheckedBigintBlockDataToByteArrayBuffer(Block block, byte[] longValueBuffer, int longBufferIndex)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            long longValue = block.getLongUnchecked(position);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            longBufferIndex += ARRAY_LONG_INDEX_SCALE;
        }

        return longBufferIndex;
    }

    private static int putAllUncheckedBigintWithNullsBlockDataToByteArrayBuffer(Block block, byte[] longValueBuffer, int longBufferIndex)
    {
        int positionCount = block.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            long longValue = block.getLongUnchecked(position);
            ByteArrayUtils.writeLong(longValueBuffer, longBufferIndex, longValue);
            if (!block.isNull(position)) {
                longBufferIndex += ARRAY_LONG_INDEX_SCALE;
            }
        }

        return longBufferIndex;
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        // Ideally we'd like multiple pages that takes up reasonable amount of memory which is larger than CPU cache size
        private static final int PAGE_COUNT = 4;
        private static final int PARTITION_COUNT = 333;
        private static final int POSITIONS_PER_PAGE = 10000;
        private static final int DICTIONARY_SIZE = 1024;

        private final int[][] positions = new int[PARTITION_COUNT][POSITIONS_PER_PAGE];
        private final int[] positionCounts = new int[PARTITION_COUNT];

        private long[][][] longValues;
        private long[][][] longValuesForDicitionary;
        private int[][][] ids;
        private boolean[][][] nulls;
        private Page[] bigintDataPages;
        private Page[] dictionaryDataPages;
        private Page[] compositeDataPages;

        private byte[][][] longValuesBuffers;
        private int[][] longValueBufferIndexes;

        private SliceOutput[][] basicSliceOutput;
        private SliceOutput[][] dynamicSliceOutput;
        private TestingSliceOutput[][] testingSliceOutput;

        @Param({"1", "2"})
        private int channelCount = 1;

        @Param({"true", "false"})
        private boolean mayHaveNull =true;

        @Setup
        public void setup()
        {
            createBigintValues();
            createBigintValuesForDictionary();
            createIds();
            createNulls();

            createBigintPages();
            createDictionaryPages();
            createCompositePages();

            createBigintByteArrayBuffers();
            createBigintByteArrayBufferIndex();

            createBasicSliceOutput();
            createDynamicSliceOutput();
            createTestingSliceOutput();

            populatePositions();
        }

        public int[][] getLongValueBufferIndexes()
        {
            for (int j = 0; j < PARTITION_COUNT; j++) {
                Arrays.fill(longValueBufferIndexes[j], 0);
            }
            return longValueBufferIndexes;
        }

        public SliceOutput getBasicSliceOutput(Integer partition, Integer channelIndex)
        {
            return basicSliceOutput[partition][channelIndex];
        }

        public SliceOutput getDynamicSliceOutput(int partition, int channelIndex)
        {
            return dynamicSliceOutput[partition][channelIndex];
        }

        public void resetSliceOutput()
        {
            for (int pageIndex = 0; pageIndex < PARTITION_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    basicSliceOutput[pageIndex][channelIndex].reset();
                    dynamicSliceOutput[pageIndex][channelIndex].reset();
                }
            }
        }

        public void resetTestingSliceOutput()
        {
            for (int pageIndex = 0; pageIndex < PARTITION_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    testingSliceOutput[pageIndex][channelIndex].reset();
                }
            }
        }

        private void createBigintPages()
        {
            bigintDataPages = new Page[PAGE_COUNT];

            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount];
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    if (mayHaveNull) {
                        blocks[channelIndex] = createBigintWithNullsChannel();
                    }
                    else {
                        blocks[channelIndex] = createBigintChannel();
                    }
                }
                bigintDataPages[pageIndex] = new Page(blocks);
            }
        }

        private void createDictionaryPages()
        {
            dictionaryDataPages = new Page[PAGE_COUNT];

            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount];
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    if (mayHaveNull) {
                        blocks[channelIndex] = createDictionaryBigintWithNullsChannel();
                    }
                    else {
                        blocks[channelIndex] = createDictionaryBigintChannel();
                    }
                }
                dictionaryDataPages[pageIndex] = new Page(blocks);
            }
        }

        private void createCompositePages()
        {
            compositeDataPages = new Page[PAGE_COUNT];

            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                Block[] blocks = new Block[channelCount * 2];
                for (int channelIndex = 0; channelIndex < channelCount * 2; channelIndex += 2) {
                    if (mayHaveNull) {
                        blocks[channelIndex] = createBigintWithNullsChannel();
                        blocks[channelIndex + 1] = createDictionaryBigintWithNullsChannel();
                    }
                    else {
                        blocks[channelIndex] = createBigintChannel();
                        blocks[channelIndex + 1] = createDictionaryBigintChannel();
                    }
                }
                compositeDataPages[pageIndex] = new Page(blocks);
            }
        }

        private Block createBigintChannel()
        {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                BIGINT.writeLong(blockBuilder, position);
            }
            return blockBuilder.build();
        }

        private Block createBigintWithNullsChannel()
        {
            BlockBuilder blockBuilder = BIGINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    BIGINT.writeLong(blockBuilder, position);
                }
                else {
                    blockBuilder.appendNull();
                }
            }
            return blockBuilder.build();
        }

        private Block createDictionaryBigintWithNullsChannel()
        {
            BlockBuilder dictionaryBuilder = BIGINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < 1024; position++) {
                if (ThreadLocalRandom.current().nextBoolean()) {
                    BIGINT.writeLong(dictionaryBuilder, position);
                }
                else {
                    dictionaryBuilder.appendNull();
                }
            }
            Block dictionary = dictionaryBuilder.build();

            int[] ids = new int[POSITIONS_PER_PAGE];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = ThreadLocalRandom.current().nextInt(1024);
            }
            return new DictionaryBlock(dictionary, ids);
        }

        private Block createDictionaryBigintChannel()
        {
            BlockBuilder dictionaryBuilder = BIGINT.createBlockBuilder(null, POSITIONS_PER_PAGE);
            for (int position = 0; position < 1024; position++) {
                BIGINT.writeLong(dictionaryBuilder, position);
            }
            Block dictionary = dictionaryBuilder.build();

            int[] ids = new int[POSITIONS_PER_PAGE];
            for (int i = 0; i < ids.length; i++) {
                ids[i] = ThreadLocalRandom.current().nextInt(DICTIONARY_SIZE);
            }
            return new DictionaryBlock(dictionary, ids);
        }

        private Block createRLEChannel()
        {
            return RunLengthEncodedBlock.create(BIGINT, 0, POSITIONS_PER_PAGE);
        }
        
        private void createBigintValues()
        {
            longValues = new long[PAGE_COUNT][channelCount][POSITIONS_PER_PAGE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                        longValues[pageIndex][channelIndex][position] = (long) position;
                    }
                }
            }
        }

        private void createBigintValuesForDictionary()
        {
            longValuesForDicitionary = new long[PAGE_COUNT][channelCount][DICTIONARY_SIZE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < DICTIONARY_SIZE; position++) {
                        longValuesForDicitionary[pageIndex][channelIndex][position] = (long) position;
                    }
                }
            }
        }

        private void createIds()
        {
            ids = new int[PAGE_COUNT][channelCount][POSITIONS_PER_PAGE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                        ids[pageIndex][channelIndex][position] = ThreadLocalRandom.current().nextInt(DICTIONARY_SIZE);
                    }
                }
            }
        }

        private void createNulls()
        {
            nulls = new boolean[PAGE_COUNT][channelCount][POSITIONS_PER_PAGE];
            for (int pageIndex = 0; pageIndex < PAGE_COUNT; pageIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    for (int position = 0; position < POSITIONS_PER_PAGE; position++) {
                        nulls[pageIndex][channelIndex][position] = ThreadLocalRandom.current().nextBoolean();
                    }
                }
            }
        }

        private void createBigintByteArrayBuffers()
        {
            longValuesBuffers = new byte[PARTITION_COUNT][channelCount][PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE];
        }

        private void createBigintByteArrayBufferIndex()
        {
            longValueBufferIndexes = new int[PARTITION_COUNT][channelCount];
        }

        private void createBasicSliceOutput()
        {
            basicSliceOutput = new BasicSliceOutput[PARTITION_COUNT][channelCount];
            for (int partitionIndex = 0; partitionIndex < PARTITION_COUNT; partitionIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    Slice slice = Slices.wrappedBuffer(new byte[PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE]);
                    basicSliceOutput[partitionIndex][channelIndex] = slice.getOutput();
                }
            }
        }

        private void createDynamicSliceOutput()
        {
            dynamicSliceOutput = new DynamicSliceOutput[PARTITION_COUNT][channelCount];
            for (int partitionIndex = 0; partitionIndex < PARTITION_COUNT; partitionIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    dynamicSliceOutput[partitionIndex][channelIndex] = new DynamicSliceOutput(1024 * ARRAY_LONG_INDEX_SCALE);
                }
            }
        }

        private void createTestingSliceOutput()
        {
            testingSliceOutput = new TestingSliceOutput[PARTITION_COUNT][channelCount];
            for (int partitionIndex = 0; partitionIndex < PARTITION_COUNT; partitionIndex++) {
                for (int channelIndex = 0; channelIndex < channelCount; channelIndex++) {
                    testingSliceOutput[partitionIndex][channelIndex] = new TestingSliceOutput(10 * PAGE_COUNT * POSITIONS_PER_PAGE * ARRAY_LONG_INDEX_SCALE);
                }
            }
        }

        private void populatePositions()
        {
            for (int i = 0; i < POSITIONS_PER_PAGE; i++) {
                int partition = ThreadLocalRandom.current().nextInt(PARTITION_COUNT);
                int indexForPartition = positionCounts[partition];
                positions[partition][indexForPartition] = i;
                positionCounts[partition]++;
            }
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyUncheckedDictionaryBigintPagesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongValuesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongValuesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongValuesToByteArrayBuffer2(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongPagesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongPagesToByteArrayBuffer2(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().addCompositeValuesToByteArrayBuffer(data);
//        new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongValuesToDynamicSliceOutput(data);
       // new BenchmarkPartitionedOutputOperatorAppendToBuffers().copyLongPagesToDynamicSliceOutput(data);
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .jvmArgs("-Xmx40g") //,
//                        "-XX:+UnlockDiagnosticVMOptions",
//                        "-XX:+PrintAssembly",
//                        "-XX:PrintAssemblyOptions=intel",
//                        "-XX:CompileCommand=print,*BenchmarkPartitionedOutputOperatorAppendToBuffers.copyCompositePagesToByteArrayBuffer")

                .include(".*" + BenchmarkPartitionedOutputOperatorAppendToBuffers.class.getSimpleName() + ".*")
                .addProfiler(LinuxPerfAsmProfiler.class, "events=cpu-clock")
                .build();
        new Runner(options).run();
    }
}
