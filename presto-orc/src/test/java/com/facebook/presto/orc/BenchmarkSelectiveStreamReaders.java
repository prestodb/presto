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

import com.facebook.presto.orc.TupleDomainFilter.BigintRange;
import com.facebook.presto.orc.TupleDomainFilter.BooleanValue;
import com.facebook.presto.orc.TupleDomainFilter.DoubleRange;
import com.facebook.presto.orc.TupleDomainFilter.FloatRange;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.SqlDate;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnHive;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
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
    public static final int ROWS = 10_000_000;
    public static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);

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
                blocks.add(page.getBlock(0));
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

        public void setup(String typeSignature)
                throws Exception
        {
            type = new TypeRegistry().getType(TypeSignature.parseTypeSignature(typeSignature));
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

        protected abstract List<?> createValues();

        public OrcSelectiveRecordReader createRecordReader(Optional<TupleDomainFilter> filter)
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));

            return orcReader.createSelectiveRecordReader(
                    ImmutableMap.of(0, type),
                    ImmutableList.of(0),
                    filter.isPresent() ? ImmutableMap.of(0, ImmutableMap.of(new Subfield("c"), filter.get())) : ImmutableMap.of(),
                    ImmutableList.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    ImmutableMap.of(),
                    OrcPredicate.TRUE,
                    0,
                    dataSource.getSize(),
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    Optional.empty(),
                    INITIAL_BATCH_SIZE);
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
                "double"
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
                "double"
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

            throw new UnsupportedOperationException("Unsupported type: " + getType());
        }
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
