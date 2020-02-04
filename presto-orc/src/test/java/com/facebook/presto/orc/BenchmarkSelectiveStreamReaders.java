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
import org.testng.annotations.Test;

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

    @Benchmark
    public List<Block> readAllBlocks(BenchmarkData data)
            throws IOException
    {
        OrcSelectiveRecordReader recordReader = data.createRecordReader();

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

    @Test
    public void verifyReadAllBlocks()
            throws Exception
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkSelectiveStreamReaders benchmarkSelectiveStreamReaders = new BenchmarkSelectiveStreamReaders();
        benchmarkSelectiveStreamReaders.readAllBlocks(data);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int NO_FILTER = -1;

        private final Random random = new Random(0);

        private Type type;
        private File temporaryDirectory;
        private File orcFile;
        private Optional<TupleDomainFilter> filter;
        private boolean filterAllowNull;
        private float selectionRateForNonNull;

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
        private String typeSignature = "boolean";

        @Param({
                "PARTIAL",
                "NONE",
                "ALL"
        })
        private Nulls withNulls = Nulls.NONE;

        // 0 means no rows will be filtered out, 1 means all rows will be filtered out, -1 means no filter.
        // When withNulls is ALL, only -1, 0, 1 are meaningful. Other values are regarded as 1.
        @SuppressWarnings("unused")
        @Param({"-1", "0", "0.1", "0.5", "0.9", "1"})
        private float filterRate = 0.1f;

        @Setup
        public void setup()
                throws Exception
        {
            if (typeSignature.startsWith("varchar")) {
                type = new TypeRegistry().getType(TypeSignature.parseTypeSignature("varchar"));
            }
            else {
                type = new TypeRegistry().getType(TypeSignature.parseTypeSignature(typeSignature));
            }

            temporaryDirectory = createTempDir();
            orcFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(orcFile, ORC_12, NONE, type, createValues());

            filter = getFilter();
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        public OrcSelectiveRecordReader createRecordReader()
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

        private Optional<TupleDomainFilter> getFilter()
        {
            if (filterRate == NO_FILTER) {
                return Optional.empty();
            }

            setupFilterRateForNonNull();

            if (type == BOOLEAN) {
                return Optional.of(BooleanValue.of(true, filterAllowNull));
            }

            if (type == BIGINT) {
                return Optional.of(BigintRange.of((long) (Long.MIN_VALUE * selectionRateForNonNull), (long) (Long.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == INTEGER || type == DATE || type == TIMESTAMP) {
                return Optional.of(BigintRange.of((long) (Integer.MIN_VALUE * selectionRateForNonNull), (long) (Integer.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == INTEGER) {
                return Optional.of(BigintRange.of((long) (Integer.MIN_VALUE * selectionRateForNonNull), (long) (Integer.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == SMALLINT) {
                return Optional.of(BigintRange.of((long) (Short.MIN_VALUE * selectionRateForNonNull), (long) (Short.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == TINYINT) {
                return Optional.of(BigintRange.of((long) (Byte.MIN_VALUE * selectionRateForNonNull), (long) (Byte.MAX_VALUE * selectionRateForNonNull), filterAllowNull));
            }

            if (type == REAL) {
                return Optional.of(FloatRange.of(0, false, false, selectionRateForNonNull, false, true, filterAllowNull));
            }

            if (type == DOUBLE) {
                return Optional.of(DoubleRange.of(0, false, false, selectionRateForNonNull, false, true, filterAllowNull));
            }

            if (type instanceof DecimalType) {
                if (((DecimalType) type).isShort()) {
                    return Optional.of(BigintRange.of((long) (-10_000_000_000L * selectionRateForNonNull), (long) (10_000_000_000L * selectionRateForNonNull), filterAllowNull));
                }
                return Optional.of(LongDecimalRange.of(
                        (long) (-10_000_000_000L * selectionRateForNonNull),
                        (long) (-10_000_000_000L * selectionRateForNonNull),
                        false,
                        true,
                        (long) (10_000_000_000L * selectionRateForNonNull),
                        (long) (10_000_000_000L * selectionRateForNonNull),
                        false,
                        true,
                        filterAllowNull));
            }

            if (type instanceof VarcharType) {
                if (typeSignature.equals("varchar_dictionary")) {
                    return Optional.of(BytesRange.of("000000000".getBytes(), false, "000000000".getBytes(), filterRate == 1 ? true : false, filterAllowNull));
                }

                return Optional.of(BytesRange.of("000000000".getBytes(), false, String.format("%09d", (int) (999_999_999 * selectionRateForNonNull) - 1).getBytes(), filterRate == 1 ? true : false, filterAllowNull));
            }

            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        private final List<?> createValues()
        {
            switch (withNulls) {
                case ALL:
                    return NULL_VALUES;
                case PARTIAL:
                    // Let the null rate be 0.5 * (1 - filterRate)
                    return IntStream.range(0, ROWS).mapToObj(i -> random.nextFloat() > 0.5 * (filterRate == -1 ? 1 : 1 - filterRate) ? createValue() : null).collect(toList());
                default:
                    return IntStream.range(0, ROWS).mapToObj(i -> createValue()).collect(toList());
            }
        }

        private final Object createValue()
        {
            if (type == BOOLEAN) {
                // We need to specialize BOOLEAN case because we can't specify filterRate by manipulating the filter value in getFilter.
                // Since the filters allows null, so all nulls would all be selected. To make the total selected positions equal to ( 1- filterRate) * positionCount,
                // we need to adapt the filterRate for non null values as follows:
                return random.nextFloat() <= (1 - filterRate) / (1 + filterRate);
            }

            if (type == BIGINT) {
                return random.nextLong();
            }

            if (type == INTEGER) {
                return random.nextInt();
            }

            if (type == SMALLINT) {
                return (short) random.nextInt();
            }

            if (type == TINYINT) {
                return (byte) random.nextInt();
            }

            if (type == DATE) {
                return new SqlDate(random.nextInt());
            }

            if (type == TIMESTAMP) {
                // We use int because longs will be converted to int when being written.
                long value = random.nextInt();
                return new SqlTimestamp(value, TimeZoneKey.UTC_KEY);
            }

            if (type == REAL) {
                return random.nextFloat();
            }

            if (type == DOUBLE) {
                return random.nextDouble();
            }

            if (type instanceof DecimalType) {
                if (Decimals.isShortDecimal(type)) {
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), SHORT_DECIMAL_TYPE.getPrecision(), SHORT_DECIMAL_TYPE.getScale());
                }
                else {
                    return new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), LONG_DECIMAL_TYPE.getPrecision(), LONG_DECIMAL_TYPE.getScale());
                }
            }

            if (type == VARCHAR) {
                if (typeSignature.equals("varchar_dictionary")) {
                    return Strings.repeat("0", 9);
                }

                return randomAsciiString(random);
            }

            throw new UnsupportedOperationException("Unsupported type: " + type);
        }

        private void setupFilterRateForNonNull()
        {
            switch (withNulls) {
                case NONE:
                    filterAllowNull = false;  // doesn't matter
                    selectionRateForNonNull = 1 - filterRate;
                    break;
                case PARTIAL:
                    filterAllowNull = true;  // doesn't matter
                    selectionRateForNonNull = (1 - filterRate) / (1 + filterRate);  // This is because we assumed the null rate is 0.5
                    break;
                case ALL:
                    filterAllowNull = filterRate == 0 ? true : false;  // filterRate belonging to (0, 1] is regarded as 1
                    selectionRateForNonNull = 1;  // This value doesn't matter
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported withNulls: " + withNulls);
            }
        }

        public enum Nulls
        {
            PARTIAL, NONE, ALL;
        }
    }

    private static String randomAsciiString(Random random)
    {
        return String.format("%09d", random.nextInt(999_999_999));
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
