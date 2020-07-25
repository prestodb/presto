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
import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.SqlTimestamp;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.BytesRange;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.orc.cache.StorageOrcFileTailSource;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Longs;
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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.orc.DwrfEncryptionProvider.NO_ENCRYPTION;
import static com.facebook.presto.orc.NoopOrcAggregatedMemoryContext.NOOP_ORC_AGGREGATED_MEMORY_CONTEXT;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnHive;
import static com.facebook.presto.orc.TupleDomainFilter.LongDecimalRange;
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
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(2)
@Warmup(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 1000, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkSelectiveStreamReaders
{
    private static final int ROWS = 10_000_000;
    private static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);
    private static final DecimalType SHORT_DECIMAL_TYPE = DecimalType.createDecimalType(10, 5);
    private static final DecimalType LONG_DECIMAL_TYPE = DecimalType.createDecimalType(30, 10);
    private static final int MAX_STRING_LENGTH = 10;

    @Benchmark
    public Object readAllNull(AllNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(Optional.empty()));
    }

    @Benchmark
    public Object read(BenchmarkData data)
            throws IOException
    {
        return readAllBlocks(data.createRecordReader(Optional.empty()));
    }

    @Benchmark
    public Object readWithFilter(BenchmarkData data)
            throws IOException
    {
        return readAllBlocks(data.createRecordReader(data.getFilter()));
    }

    private static List<Block> readAllBlocks(OrcSelectiveRecordReader recordReader)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        while (true) {
            Page page = recordReader.getNextPage();
            if (page == null) {
                break;
            }

            if (page.getPositionCount() > 0) {
                blocks.add(page.getBlock(0).getLoadedBlock());
            }
        }
        return blocks;
    }

    private abstract static class AbstractBenchmarkData
    {
        protected final Random random = new Random(0);
        private Type type;
        private File temporaryDirectory;
        private File orcFile;
        private String typeSignature;

        public void setup(String typeSignature)
                throws Exception
        {
            if (typeSignature.startsWith("varchar")) {
                type = new TypeRegistry().getType(TypeSignature.parseTypeSignature("varchar"));
            }
            else {
                type = new TypeRegistry().getType(TypeSignature.parseTypeSignature(typeSignature));
            }
            this.typeSignature = typeSignature;
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

        public Type getType()
        {
            return type;
        }

        public String getTypeSignature()
        {
            return typeSignature;
        }

        protected abstract List<?> createValues();

        public OrcSelectiveRecordReader createRecordReader(Optional<TupleDomainFilter> filter)
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
                    NO_ENCRYPTION);

            return orcReader.createSelectiveRecordReader(
                    ImmutableMap.of(0, type),
                    ImmutableList.of(0),
                    filter.isPresent() ? ImmutableMap.of(0, ImmutableMap.of(new Subfield("c"), filter.get())) : ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    OrcPredicate.TRUE,
                    0,
                    dataSource.getSize(),
                    UTC, // arbitrary
                    true,
                    new TestingHiveOrcAggregatedMemoryContext(),
                    Optional.empty(),
                    INITIAL_BATCH_SIZE,
                    ImmutableMap.of());
        }
    }

    @State(Scope.Thread)
    public static class AllNullBenchmarkData
            extends AbstractBenchmarkData
    {
        @SuppressWarnings("unused")
        @Param({
                "boolean",

                "integer",
                "bigint",
                "smallint",
                "tinyint",

                "date",
                "timestamp",

                "real",
                "double",
                "decimal(10,5)",
                "decimal(30,10)",

                "varchar_direct",
                "varchar_dictionary"
        })
        private String typeSignature;

        @Setup
        public void setup()
                throws Exception
        {
            setup(typeSignature);
        }

        @Override
        protected final List<?> createValues()
        {
            return NULL_VALUES;
        }
    }

    @State(Scope.Thread)
    public static class BenchmarkData
            extends AbstractBenchmarkData
    {
        @SuppressWarnings("unused")
        @Param({
                "boolean",

                "integer",
                "bigint",
                "smallint",
                "tinyint",

                "date",
                "timestamp",

                "real",
                "double",
                "decimal(10,5)",
                "decimal(30,10)",

                "varchar_direct",
                "varchar_dictionary"
        })
        private String typeSignature;

        @SuppressWarnings("unused")
        @Param({"true", "false"})
        private boolean withNulls;

        private Optional<TupleDomainFilter> filter;

        @Setup
        public void setup()
                throws Exception
        {
            setup(typeSignature);

            this.filter = getFilter(getType());
        }

        public Optional<TupleDomainFilter> getFilter()
        {
            return filter;
        }

        private static Optional<TupleDomainFilter> getFilter(Type type)
        {
            if (type == BOOLEAN) {
                return Optional.of(BooleanValue.of(true, true));
            }

            if (type == TINYINT || type == BIGINT || type == INTEGER || type == SMALLINT || type == DATE || type == TIMESTAMP) {
                return Optional.of(BigintRange.of(0, Long.MAX_VALUE, true));
            }

            if (type == REAL) {
                return Optional.of(FloatRange.of(0, true, true, Integer.MAX_VALUE, true, true, true));
            }

            if (type == DOUBLE) {
                return Optional.of(DoubleRange.of(.5, false, false, 2, false, false, false));
            }

            if (type instanceof DecimalType) {
                if (((DecimalType) type).isShort()) {
                    return Optional.of(BigintRange.of(0, Long.MAX_VALUE, true));
                }
                return Optional.of(LongDecimalRange.of(0, 0, false, true, Long.MAX_VALUE, Long.MAX_VALUE, false, true, true));
            }

            if (type instanceof VarcharType) {
                return Optional.of(BytesRange.of("0".getBytes(), false, Longs.toByteArray(Long.MAX_VALUE), false, true));
            }

            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        @Override
        protected final List<?> createValues()
        {
            if (withNulls) {
                return IntStream.range(0, ROWS).mapToObj(i -> i % 2 == 0 ? createValue() : null).collect(toList());
            }
            return IntStream.range(0, ROWS).mapToObj(i -> createValue()).collect(toList());
        }

        private final Object createValue()
        {
            if (getType() == BOOLEAN) {
                return random.nextBoolean();
            }

            if (getType() == BIGINT) {
                return random.nextLong();
            }

            if (getType() == INTEGER) {
                return random.nextInt();
            }

            if (getType() == SMALLINT) {
                return (short) random.nextInt();
            }

            if (getType() == TINYINT) {
                return (byte) random.nextInt();
            }

            if (getType() == DATE) {
                return new SqlDate(random.nextInt());
            }

            if (getType() == TIMESTAMP) {
                return new SqlTimestamp(random.nextLong(), TimeZoneKey.UTC_KEY);
            }

            if (getType() == REAL) {
                return random.nextFloat();
            }

            if (getType() == DOUBLE) {
                return random.nextDouble();
            }

            if (getType() instanceof DecimalType) {
                if (Decimals.isShortDecimal(getType())) {
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), SHORT_DECIMAL_TYPE.getPrecision(), SHORT_DECIMAL_TYPE.getScale());
                }
                else {
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), LONG_DECIMAL_TYPE.getPrecision(), LONG_DECIMAL_TYPE.getScale());
                }
            }

            if (getType() == VARCHAR) {
                if (typeSignature.equals("varchar_dictionary")) {
                    return Strings.repeat("0", MAX_STRING_LENGTH);
                }
                return randomAsciiString(random, MAX_STRING_LENGTH);
            }

            throw new UnsupportedOperationException("Unsupported type: " + getType());
        }
    }

    private static String randomAsciiString(Random random, int maxLength)
    {
        char[] value = new char[random.nextInt(maxLength)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkSelectiveStreamReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
