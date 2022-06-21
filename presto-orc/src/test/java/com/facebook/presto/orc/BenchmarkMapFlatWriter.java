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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.io.OutputStreamDataSink;
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
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.OrcTester.HIVE_STORAGE_TIME_ZONE;
import static com.facebook.presto.orc.OrcTester.mapType;
import static com.facebook.presto.orc.OrcWriteValidation.OrcWriteValidationMode.BOTH;
import static com.facebook.presto.orc.metadata.CompressionKind.ZSTD;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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
                new NoOpOrcWriterStats());

        for (Block block : data.blocks) {
            writer.write(new Page(block));
        }

        writer.close();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final Random random = new Random(SEED);

        private Type type;
        private File temporaryDirectory;
        private File orcFile;

        @Param("bigint")
        private String keyType = "bigint";

        @Param("bigint")
        private String valueType = "bigint";

        @Param("5000")
        private int distinctKeyCount = 5000;

        @Param("50000")
        private int positionsPerBlock = 50000;

        @Param("500")
        private int maxKeysPerEntry = 500;

        @Param("5")
        private int blockCount = 5;

        private List<Block> blocks;

        @Setup
        public void setup()
                throws Exception
        {
            FunctionAndTypeManager testFunctionAndTypeManager = createTestFunctionAndTypeManager();
            Type keyType = testFunctionAndTypeManager.getType(parseTypeSignature(this.keyType));
            Type valueType = testFunctionAndTypeManager.getType(parseTypeSignature(this.valueType));

            this.type = mapType(keyType, valueType);
            this.temporaryDirectory = createTempDir();
            this.orcFile = new File(temporaryDirectory, randomUUID().toString());

            ImmutableList.Builder<Block> blocks = ImmutableList.builder();

            List<Long> keys = new ArrayList<>();
            for (int i = 0; i < distinctKeyCount; i++) {
                keys.add((long) i);
            }

            for (int i = 0; i < blockCount; i++) {
                MapBlockBuilder mapBlockBuilder = (MapBlockBuilder) type.createBlockBuilder(null, positionsPerBlock);
                for (int j = 0; j < positionsPerBlock; j++) {
                    Collections.shuffle(keys, random);
                    int entryKeyCount = random.nextInt(maxKeysPerEntry);

                    mapBlockBuilder.beginDirectEntry();
                    BlockBuilder keyBuilder = mapBlockBuilder.getKeyBlockBuilder();
                    BlockBuilder valueBuilder = mapBlockBuilder.getValueBlockBuilder();

                    for (int k = 0; k < entryKeyCount; k++) {
                        keyType.writeLong(keyBuilder, keys.get(k));
                        keyType.writeLong(valueBuilder, keys.get(k) * 2L + 1);
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
