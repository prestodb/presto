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
import com.facebook.presto.orc.checkpoint.LongStreamCheckpoint;
import com.facebook.presto.orc.metadata.CompressionKind;
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
import java.util.EnumMap;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcDecompressor.createOrcDecompressor;
import static com.facebook.presto.orc.metadata.OrcType.OrcTypeKind.LONG;
import static com.facebook.presto.orc.metadata.Stream.StreamKind.DATA;
import static com.facebook.presto.orc.stream.AbstractTestValueStream.COMPRESSION_BLOCK_SIZE;
import static com.facebook.presto.orc.stream.AbstractTestValueStream.ORC_DATA_SOURCE_ID;
import static org.testng.Assert.assertEquals;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@Fork(5)
@Warmup(iterations = 20, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 50, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkLongInputStream
{
    private static final List<List<Long>> GROUPS;
    private static final EnumMap<CompressionKind, EnumMap<Encoding, Slice>> SLICES = new EnumMap<>(CompressionKind.class);
    private static final EnumMap<CompressionKind, EnumMap<Encoding, List<LongStreamCheckpoint>>> CHECKPOINTS = new EnumMap<>(CompressionKind.class);

    static {
        GROUPS = getData();
        for (CompressionKind kind : CompressionKind.values()) {
            for (Encoding encoding : Encoding.values()) {
                LongOutputStream outputStream = createOutputStream(kind, encoding);
                List<LongStreamCheckpoint> checkpoints = writeData(outputStream, GROUPS);
                CHECKPOINTS.computeIfAbsent(kind, ignored -> new EnumMap<>(Encoding.class)).put(encoding, checkpoints);
                SLICES.computeIfAbsent(kind, ignored -> new EnumMap<>(Encoding.class)).put(encoding, getSlice(outputStream));
            }
        }
    }

    @Benchmark
    @OperationsPerInvocation(10)
    public Object benchmarkNext(BenchmarkData data)
            throws IOException
    {
        int index = 0;
        for (List<Long> group : GROUPS) {
            for (Long expectedValue : group) {
                index++;
                Long actualValue = data.inputStream.next();
                if (!actualValue.equals(expectedValue)) {
                    assertEquals(actualValue, expectedValue);
                }
            }
        }
        return index;
    }

    @Benchmark
    @OperationsPerInvocation(10)
    public Object benchmarkCheckpoints(BenchmarkData data)
            throws IOException
    {
        int index = 0;
        for (int groupIndex = 0; groupIndex < GROUPS.size(); groupIndex++) {
            data.inputStream.seekToCheckpoint(CHECKPOINTS.get(CompressionKind.valueOf(data.compressionKind)).get(Encoding.valueOf(data.encoding)).get(groupIndex));
            for (Long expectedValue : GROUPS.get(groupIndex)) {
                index++;
                Long actualValue = data.inputStream.next();
                if (!actualValue.equals(expectedValue)) {
                    assertEquals(actualValue, expectedValue);
                }
            }
        }
        return index;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BenchmarkData
    {
        @Param({"V1", "V2", "DWRF"})
        String encoding = "DWRF";

        @Param({"ZSTD", "ZLIB"})
        String compressionKind = "ZLIB";

        @Param({"false", "true"})
        String orcOptimizedReaderEnabled = "false";

        LongInputStream inputStream;

        @Setup(Level.Invocation)
        public void setup()
                throws IOException
        {
            inputStream = createValueStream(CompressionKind.valueOf(compressionKind), Encoding.valueOf(encoding), Boolean.valueOf(orcOptimizedReaderEnabled));
        }
    }

    private static LongInputStream createValueStream(CompressionKind kind, Encoding encoding, boolean orcOptimizedReaderEnabled)
            throws IOException
    {
        Slice slice = SLICES.get(kind).get(encoding);
        Optional<OrcDecompressor> orcDecompressor = createOrcDecompressor(ORC_DATA_SOURCE_ID, kind, COMPRESSION_BLOCK_SIZE);
        OrcInputStream input;
        if (orcOptimizedReaderEnabled) {
            input = new OptimizedOrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        }
        else {
            input = new LegacyOrcInputStream(ORC_DATA_SOURCE_ID, slice.getInput(), orcDecompressor, newSimpleAggregatedMemoryContext(), slice.getRetainedSize());
        }
        switch (encoding) {
            case V1:
                if (orcOptimizedReaderEnabled) {
                    return new OptimizedLongInputStreamV1(input, true);
                }
                else {
                    return new LongInputStreamV1(input, true);
                }
            case V2:
                return new LongInputStreamV2(input, true, true);
            case DWRF:
                if (orcOptimizedReaderEnabled) {
                    return new OptimizedLongInputStreamDwrf(input, LONG, true, true);
                }
                else {
                    return new LongInputStreamDwrf(input, LONG, true, true);
                }
            default:
                throw new IllegalStateException();
        }
    }
    private enum Encoding {
        V1, V2, DWRF
    }

    private static List<LongStreamCheckpoint> writeData(LongOutputStream outputStream, List<List<Long>> groups)
    {
        for (List<Long> group : groups) {
            outputStream.recordCheckpoint();
            group.forEach(outputStream::writeLong);
        }
        outputStream.close();
        return outputStream.getCheckpoints();
    }

    public static Slice getSlice(LongOutputStream outputStream)
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(1000);
        StreamDataOutput streamDataOutput = outputStream.getStreamDataOutput(33);
        streamDataOutput.writeData(sliceOutput);
        return sliceOutput.slice();
    }

    private static LongOutputStream createOutputStream(CompressionKind kind, Encoding encoding)
    {
        switch (encoding) {
            case V1:
                return new LongOutputStreamV1(kind, COMPRESSION_BLOCK_SIZE, true, DATA);
            case V2:
                return new LongOutputStreamV2(kind, COMPRESSION_BLOCK_SIZE, true, DATA);
            case DWRF:
                return new LongOutputStreamDwrf(kind, COMPRESSION_BLOCK_SIZE, true, DATA);
            default:
                throw new IllegalStateException();
        }
    }

    private static List<List<Long>> getData()
    {
        List<List<Long>> groups = new ArrayList<>();
        List<Long> group;

        group = new ArrayList<>();
        Random random = new Random(22);
        for (int i = 0; i < 70000; i++) {
            group.add(-1000L + random.nextInt(17));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            group.add((long) (i));
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            group.add((long) (10_000 + (i * 17)));
        }
        groups.add(group);

        group = new ArrayList<>();
        long base = 5_900_000_000_000L;
        long value = 0;
        for (int i = 0; i < 50000; i++) {
            group.add(base + value);
            value += 2 * i + 1;
        }
        groups.add(group);

        group = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            group.add((long) (10_000 - (i * 17)));
        }
        groups.add(group);
        return groups;
    }

    public static void main(String[] args)
            throws Throwable
    {
        // assure the benchmarks are valid before running

        /*BenchmarkData data = new BenchmarkData();
        data.setup();
        new BenchmarkLongInputStream().benchmarkNext(data);
        data.setup();
        new BenchmarkLongInputStream().benchmarkCheckpoints(data);
        */
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkLongInputStream.class.getSimpleName() + ".*")
                .jvmArgs("-Xmx5g")
                .build();
        new Runner(options).run();
    }
}
