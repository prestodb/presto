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

import com.facebook.presto.orc.OrcDecompressor;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.Stream;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.stream.AbstractTestValueStream.COMPRESSION_BLOCK_SIZE;
import static com.facebook.presto.orc.stream.AbstractTestValueStream.ORC_DATA_SOURCE_ID;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongStreamV1
{
    private static final List<List<Long>> GROUPS = getData();

    @Benchmark
    @OperationsPerInvocation(2)
    public Object benchmarkDecoding(BenchmarkData data)
            throws IOException
    {
        long actualValue = 0L;
        //data.input = getInputStream(data.slice, CompressionKind.valueOf(data.kind));
        //data.input.setPositionsFilter(data.positions, data.useLegacy);
        for (int i = 0; i < data.positions.length; i++) {
            actualValue = data.input.nextLong();
            /*
            try {
                long expectedValue = data.data.get(data.positions[i]);
                actualValue = data.input.nextLong();
                assertEquals(actualValue, expectedValue, "index=" + data.positions[i]);
            }
            catch (IOException t) {
                for (int j = Math.max(0, i - 10); j <= i; j++) {
                    System.err.println("POSITION " + j + ", " + data.positions[j]);
                }
                throw t;
            }
            */
        }
        return actualValue;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"false", "true"})
        boolean useLegacy;

        @Param({"NONE", "SNAPPY", "ZSTD", "ZLIB", "LZ4"})
        String kind = "NONE";

        @Param({"ALL", "ODD", "SECOND_HALF", "RANDOM_QUARTER"})
        String positionsType = "RANDOM_QUARTER";

        LongInputStreamV1 input;

        int[] positions;
        Slice slice;

        List<Long> data;

        @Setup(Level.Invocation)
        public void setup()
                throws IOException
        {
            this.data = GROUPS.stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableList());

            CompressionKind compressionKind = CompressionKind.valueOf(kind);
            this.slice = getInput(GROUPS, compressionKind);
            this.positions = getPositions(data, PositionsType.valueOf(positionsType));
            input = getInputStream(slice, compressionKind);
            input.setPositionsFilter(positions, useLegacy);
        }
    }

    public static Slice getInput(List<List<Long>> groups, CompressionKind kind)
    {
        LongOutputStreamV1 outputStream = new LongOutputStreamV1(kind, COMPRESSION_BLOCK_SIZE, true, DATA);
        outputStream.reset();
        for (List<Long> group : groups) {
            outputStream.recordCheckpoint();
            group.forEach(outputStream::writeLong);
        }
        outputStream.close();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(1);
        streamDataOutput.writeData(sliceOutput);
        Stream stream = streamDataOutput.getStream();
        assertEquals(stream.getStreamKind(), DATA);
        assertEquals(stream.getColumn(), 1);
        assertEquals(stream.getLength(), sliceOutput.size());
        return sliceOutput.slice();
    }

    public static LongInputStreamV1 getInputStream(Slice slice, CompressionKind kind)
            throws IOException
    {
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, kind, COMPRESSION_BLOCK_SIZE);
        OrcInputStream input = new OrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        return new LongInputStreamV1(input, true);
    }

    public static List<List<Long>> getData()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;
        for (int j = 0; j < 3; j++) {
            group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((long) (i));
            }
            groups.add(group);

            group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((long) (10_000 + (i * 17)));
            }
            groups.add(group);

            group = new ArrayList<>();
            for (int i = 0; i < 1000; i++) {
                group.add((long) (10_000 - (i * 17)));
            }
            groups.add(group);

            group = new ArrayList<>();
            Random random = new Random(22);
            for (int i = 0; i < 1000; i++) {
                group.add(-1000L + random.nextInt(17));
            }
            groups.add(group);
        }
        return groups;
    }

    public static int[] getPositions(List<Long> data, PositionsType positionsType)
    {
        int size = data.size();
        int[] positions;
        switch (positionsType) {
            case ALL:
                positions = IntStream.range(0, size).toArray();
                break;
            case ODD:
                int oddCount = (size & 1) == 1 ? (size - 1) >> 1 : size >> 1;
                positions = new int[oddCount];
                for (int i = 0; i < oddCount; i++) {
                    positions[i] = 2 * i + 1;
                }
                break;
            case SECOND_HALF:
                int mid = size >> 1;
                positions = new int[size - mid];
                for (int i = mid; i < size; i++) {
                    positions[i - mid] = i;
                }
                break;
            case RANDOM_QUARTER:
                List<Integer> all = IntStream.range(0, size).boxed().collect(toList());
                Collections.shuffle(all);
                int quarterSize = all.size() >> 2;
                positions = new int[quarterSize];
                for (int i = 0; i < quarterSize; i++) {
                    positions[i] = all.get(i);
                }
                Arrays.sort(positions);
                break;
            default:
                throw new IllegalStateException("Invalid PositionsType");
        }
        return positions;
    }
    public enum PositionsType
    {
        ALL,
        ODD,
        SECOND_HALF,
        RANDOM_QUARTER
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkLongStreamV1().benchmarkDecoding(data);

        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkLongStreamV1.class.getSimpleName() + ".*")
                .jvmArgs("-Xmx5g")
                .build();
        new Runner(options).run();
    }
}
