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

import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
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
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnHive;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.joda.time.DateTimeZone.UTC;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBatchStreamReaders
{
    private static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(10, 5);
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(30, 10);
    private static final int ROWS = 10_000_000;
    private static final int MAX_STRING = 10;
    private static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);

    @Benchmark
    public Object readBlocks(BenchmarkData data)
            throws Throwable
    {
        OrcBatchRecordReader recordReader = data.createRecordReader();
        ImmutableList.Builder<Block> blocks = new ImmutableList.Builder<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(0);
            blocks.add(block);
        }
        return blocks.build();
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private final Random random = new Random(0);

        private Type type;
        private File temporaryDirectory;
        private File orcFile;

        @SuppressWarnings("unused")
        @Param({
                "boolean",
                "tinyint",
                "smallint",
                "integer",
                "bigint",
                "decimal(10,5)",
                "decimal(30,10)",
                "timestamp",
                "real",
                "double",
                "varchar_direct",
                "varchar_dictionary"
        })
        private String typeSignature;

        @SuppressWarnings("unused")
        @Param({
                "PARTIAL",
                "NONE",
                "ALL"
        })
        private Nulls withNulls;

        @Setup
        public void setup()
                throws Exception
        {
            if (typeSignature.startsWith("varchar")) {
                type = createTestFunctionAndTypeManager().getType(TypeSignature.parseTypeSignature("varchar"));
            }
            else {
                type = createTestFunctionAndTypeManager().getType(TypeSignature.parseTypeSignature(typeSignature));
            }

            temporaryDirectory = createTempDir();
            orcFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(orcFile, ORC_12, NONE, type, createValues());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        protected List<?> createValues()
        {
            switch (withNulls) {
                case ALL:
                    return NULL_VALUES;
                case PARTIAL:
                    return IntStream.range(0, ROWS).mapToObj(i -> i % 2 == 0 ? createValue() : null).collect(toList());
                default:
                    return IntStream.range(0, ROWS).mapToObj(i -> createValue()).collect(toList());
            }
        }

        private Object createValue()
        {
            switch (typeSignature) {
                case "boolean":
                    return random.nextBoolean();
                case "tinyint":
                    return Long.valueOf(random.nextLong()).byteValue();
                case "smallint":
                    return (short) random.nextInt();
                case "integer":
                    return random.nextInt();
                case "bigint":
                    return random.nextLong();
                case "decimal(10,5)":
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), SHORT_DECIMAL_TYPE.getPrecision(), SHORT_DECIMAL_TYPE.getScale());
                case "decimal(30,10)":
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), LONG_DECIMAL_TYPE.getPrecision(), LONG_DECIMAL_TYPE.getScale());
                case "timestamp":
                    return new SqlTimestamp((random.nextLong()), UTC_KEY);
                case "real":
                    return random.nextFloat();
                case "double":
                    return random.nextDouble();
                case "varchar_dictionary":
                    return Strings.repeat("0", MAX_STRING);
                case "varchar_direct":
                    return randomAsciiString(random);
            }

            throw new UnsupportedOperationException("Unsupported type: " + typeSignature);
        }

        private OrcBatchRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(
                    dataSource,
                    ORC,
                    new StorageOrcFileTailSource(),
                    new StorageStripeMetadataSource(),
                    NOOP_ORC_AGGREGATED_MEMORY_CONTEXT,
                    OrcReaderTestingUtils.createDefaultTestConfig(),
                    false,
                    NO_ENCRYPTION,
                    DwrfKeyProvider.EMPTY,
                    new RuntimeStats());
            return orcReader.createBatchRecordReader(
                    ImmutableMap.of(0, type),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    new TestingHiveOrcAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private static String randomAsciiString(Random random)
        {
            char[] value = new char[random.nextInt(MAX_STRING)];
            for (int i = 0; i < value.length; i++) {
                value[i] = (char) random.nextInt(Byte.MAX_VALUE);
            }
            return new String(value);
        }

        public enum Nulls
        {
            PARTIAL, NONE, ALL;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBatchStreamReaders.class.getSimpleName() + ".*")
                .warmupMode(WarmupMode.BULK)
                .build();

        new Runner(options).run();
    }
}
