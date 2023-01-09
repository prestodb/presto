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
package com.facebook.presto.orc;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.io.OutputStreamDataSink;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoOpOrcWriterStats.NOOP_WRITER_STATS;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkMapFlatWriter
{
    private static final int SEED = 0;

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(BenchmarkMapFlatWriter.class.getName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Benchmark
    public void baseline(BenchmarkData data)
            throws IOException
    {
        doWrite(data);
    }

    private void doWrite(BenchmarkData data)
            throws IOException
    {
        OrcWriterOptions orcWriterOptions = OrcWriterOptions.builder()
                .withFlattenedColumns(ImmutableSet.of(0))
                .build();

        OrcWriter writer = new OrcWriter(
                new OutputStreamDataSink(new FileOutputStream(data.orcFile)),
                ImmutableList.of("col1"),
                ImmutableList.of(data.type),
                DWRF,
                ZSTD,
                Optional.empty(),
                NO_ENCRYPTION,
                orcWriterOptions,
                ImmutableMap.of(),
                HIVE_STORAGE_TIME_ZONE,
                false,
                BOTH,
                NOOP_WRITER_STATS);

        for (Block block : data.blocks) {
            writer.write(new Page(block));
        }

        writer.close();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private MapType type;
        private File temporaryDirectory;
        private File orcFile;

        @Param({"map<bigint,bigint>", "map<bigint,array<bigint>>"})
        private String typeSignature = "map<bigint,bigint>";

        // total number of keys
        @Param("500")
        private int distinctKeyCount = 500;

        // how many positions each block will have
        @Param("50000")
        private int positionsPerBlock = 50000;

        // how many blocks to create
        @Param("5")
        private int blockCount = 5;

        // number of values for the complex value types
        @Param("10")
        private int childElementValueCount = 10;

        // Weights for key frequencies bucketed by 10%.
        // It has 11 elements. The element position is the frequency bucket.
        // The value of the element is the weight indicating how many keys should
        // fall into this frequency bucket. All weights will be normalized before usage.
        // The last element is the weight for keys with 100% frequency.
        //
        // Illustration:
        // [this many keys (after weights normalization) come with frequency <10%, ... with frequency <20%, ..., ...with frequency 100%]
        //
        // For example:
        // [1, 1, 0,0,0,0,0,0,0,0, 1] means that 1/3 of keys would come with 10% frequency, another 1/3 with 20% frequency, and the last 1/3 with 100%.
        @Param("12, 2, 5, 3, 4, 5, 2, 10, 7, 45, 0")
        private String frequencyHistogram = "12, 2, 5, 3, 4, 5, 2, 10, 7, 45, 0";

        private List<Block> blocks;

        @Setup
        public void setup()
                throws Exception
        {
            Random random = new Random(SEED);

            FunctionAndTypeManager typeManager = createTestFunctionAndTypeManager();
            this.type = (MapType) typeManager.getType(parseTypeSignature(typeSignature));
            Type keyType = type.getKeyType();
            Type valueType = type.getValueType();

            this.temporaryDirectory = createTempDir();
            this.orcFile = new File(temporaryDirectory, randomUUID().toString());
            this.orcFile.deleteOnExit();

            ImmutableList.Builder<Block> blocks = ImmutableList.builder();

            List<Long> keys = new ArrayList<>(distinctKeyCount);
            for (int i = 0; i < distinctKeyCount; i++) {
                keys.add((long) i);
            }
            Collections.shuffle(keys, random);

            List<Integer> weights = Arrays.stream(frequencyHistogram.split(","))
                    .map(String::trim)
                    .map(Integer::valueOf)
                    .collect(Collectors.toList());
            assertEquals(weights.size(), 11, "Number of weights is expected to be 11");

            double weightSum = weights.stream().mapToInt(Integer::intValue).sum();
            double[] keyFrequencies = new double[distinctKeyCount];
            int offset = 0;
            for (int i = 0; i < weights.size(); i++) {
                double frequency = 0.1 * (i + 1);
                int weight = weights.get(i);
                int keyCount = (int) (distinctKeyCount * weight / weightSum);

                // select at least one element if the weight is not 0
                if (weight > 0) {
                    keyCount = Math.max(keyCount, 1);
                }

                // deal with a tiny probability of coming out of range because
                // of how weights split all keys into sub-intervals
                if (offset + keyCount >= distinctKeyCount) {
                    keyCount = distinctKeyCount - offset;
                }

                for (int j = 0; j < keyCount; j++) {
                    keyFrequencies[offset++] = frequency;
                }
            }

            for (int i = 0; i < blockCount; i++) {
                MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) this.type.createBlockBuilder(null, positionsPerBlock);
                BlockBuilder mapKeyBuilder = mapBlockBuilder.getKeyBlockBuilder();
                BlockBuilder mapValueBuilder = mapBlockBuilder.getValueBlockBuilder();

                for (int j = 0; j < positionsPerBlock; j++) {
                    mapBlockBuilder.beginDirectEntry();

                    int entryKeyCount = 0;

                    // add keys
                    for (int k = 0; k < distinctKeyCount; k++) {
                        if (random.nextDouble() < keyFrequencies[k]) {
                            keyType.writeLong(mapKeyBuilder, keys.get(k));
                            entryKeyCount++;
                        }
                    }

                    // add values
                    if (valueType == BigintType.BIGINT) {
                        for (int k = 0; k < entryKeyCount; k++) {
                            valueType.writeLong(mapValueBuilder, random.nextLong());
                        }
                    }
                    else if (valueType instanceof ArrayType) {
                        ArrayType arrayValueType = (ArrayType) valueType;
                        Type elementType = arrayValueType.getElementType();
                        ArrayBlockBuilder arrayBlockBuilder = (ArrayBlockBuilder) mapValueBuilder;
                        BlockBuilder arrayElementValueBuilder = arrayBlockBuilder.getElementBlockBuilder();
                        for (int k = 0; k < entryKeyCount; k++) {
                            arrayBlockBuilder.beginDirectEntry();
                            for (int c = 0; c < childElementValueCount; c++) {
                                elementType.writeLong(arrayElementValueBuilder, random.nextLong());
                            }
                            arrayBlockBuilder.closeEntry();
                        }
                    }
                    else {
                        throw new UnsupportedOperationException("Unsupported type: " + type);
                    }

                    mapBlockBuilder.closeEntry();
                }

                Block block = mapBlockBuilder.build();
                blocks.add(block);
            }
            this.blocks = blocks.build();
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }
    }
}
