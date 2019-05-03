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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.SqlDecimal;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnPresto;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Files.createTempDir;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.toIntExact;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkStreamReaders.ROWS)
public class BenchmarkStreamReaders
{
    private static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(10, 5);
    private static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(30, 5);
    public static final int ROWS = 10_000_000;
    private static final int DICTIONARY = 22;
    private static final int MAX_STRING = 19;
    public static final Collection<?> NULL_VALUES = Collections.nCopies(ROWS, null);

    @Benchmark
    public Object readBooleanNoNull(BooleanNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readBooleanWithNull(BooleanWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readAllNull(AllNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readByteNoNull(TinyIntNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readByteWithNull(TinyIntWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readShortDecimalNoNull(ShortDecimalNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readShortDecimalWithNull(ShortDecimalWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }
    @Benchmark
    public Object readLongDecimalNoNull(LongDecimalNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readLongDecimalWithNull(LongDecimalWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readDoubleNoNull(DoubleNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readDoubleWithNull(DoubleWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readFloatNoNull(FloatNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readFloatWithNull(FloatWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readLongNoNull(BigintNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readLongWithNull(BigintWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readIntNoNull(IntegerNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readIntWithNull(IntegerWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readShortNoNull(SmallintNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readShortWithNull(SmallintWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readSliceDirectNoNull(VarcharDirectNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readSliceDirectWithNull(VarcharDirectWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readSliceDictionaryNoNull(VarcharNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readSliceDictionaryWithNull(VarcharWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readTimestampNoNull(TimestampNoNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readTimestampWithNull(TimestampWithNullBenchmarkData data)
            throws Throwable
    {
        try (OrcRecordReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(0);
                blocks.add(block);
            }
            return blocks;
        }
    }

    private abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private Type type;
        private File temporaryDirectory;
        private File orcFile;
        private OrcDataSource dataSource;

        public void setup(Type type)
                throws Exception
        {
            this.type = type;
            temporaryDirectory = createTempDir();
            orcFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnPresto(orcFile, ORC_12, NONE, type, createValues(), new OrcWriterStats());

            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(8, MEGABYTE), new DataSize(8, MEGABYTE), true);
            DiskRange diskRange = new DiskRange(0, toIntExact(dataSource.getSize()));
            dataSource = new CachingOrcDataSource(dataSource, desiredOffset -> diskRange);
            this.dataSource = dataSource;
        }

        public Type getType()
        {
            return type;
        }

        protected abstract Iterator<?> createValues();

        OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, type),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }
    }

    @State(Scope.Thread)
    public static class AllNullBenchmarkData
            extends BenchmarkData
    {
        @SuppressWarnings("unused")
        @Param({
                "boolean",

                "tinyint",
                "integer",
                "bigint",
                "decimal(10,5)",

                "timestamp",

                "real",
                "double",

                "varchar",
                "varbinary",
        })
        private String typeSignature;

        @Setup
        public void setup()
                throws Exception
        {
            Type type = new TypeRegistry().getType(TypeSignature.parseTypeSignature(typeSignature));
            setup(type);
        }

        @Override
        protected final Iterator<?> createValues()
        {
            return NULL_VALUES.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BooleanNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean());
            }
            return values.iterator();
        }
    }
    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BooleanWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BOOLEAN);
        }

        @Override
        public Iterator<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean() ? random.nextBoolean() : null);
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class TinyIntNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Long.valueOf(random.nextLong()).byteValue());
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class TinyIntWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TINYINT);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(null);
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class ShortDecimalNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(SHORT_DECIMAL_TYPE);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), 10, 5));
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class ShortDecimalWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(SHORT_DECIMAL_TYPE);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10000000000L), 10, 5));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class LongDecimalNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(new BigInteger(96, random), 30, 5));
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class LongDecimalWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(LONG_DECIMAL_TYPE);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(new BigInteger(96, random), 30, 5));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class DoubleNoNullBenchmarkData
            extends BenchmarkData
    {
        protected File temporaryDirectory;
        protected File doubleNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            setup(DOUBLE);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextDouble());
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class DoubleWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(DOUBLE);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextDouble());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class FloatNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(REAL);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextFloat());
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class FloatWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(REAL);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextFloat());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BigintNoNullBenchmarkData
            extends BenchmarkData
    {
        public static BigintNoNullBenchmarkData create()
                throws Exception
        {
            BigintNoNullBenchmarkData data = new BigintNoNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextLong());
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BigintWithNullBenchmarkData
            extends BenchmarkData
    {
        public static BigintWithNullBenchmarkData create()
                throws Exception
        {
            BigintWithNullBenchmarkData data = new BigintWithNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(null);
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class IntegerNoNullBenchmarkData
            extends BenchmarkData
    {
        public static IntegerNoNullBenchmarkData create()
                throws Exception
        {
            IntegerNoNullBenchmarkData data = new IntegerNoNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(INTEGER);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextInt());
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class IntegerWithNullBenchmarkData
            extends BenchmarkData
    {
        public static IntegerWithNullBenchmarkData create()
                throws Exception
        {
            IntegerWithNullBenchmarkData data = new IntegerWithNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(INTEGER);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(random.nextInt());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class SmallintNoNullBenchmarkData
            extends BenchmarkData
    {
        public static SmallintNoNullBenchmarkData create()
                throws Exception
        {
            SmallintNoNullBenchmarkData data = new SmallintNoNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(SMALLINT);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Short> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add((short) random.nextInt());
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class SmallintWithNullBenchmarkData
            extends BenchmarkData
    {
        public static SmallintWithNullBenchmarkData create()
                throws Exception
        {
            SmallintWithNullBenchmarkData data = new SmallintWithNullBenchmarkData();
            data.setup();
            return data;
        }

        @Setup
        public void setup()
                throws Exception
        {
            setup(SMALLINT);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<Short> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add((short) random.nextInt());
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class VarcharDirectNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(randomAsciiString(random));
            }
            return values.iterator();
        }
    }

    @State(Scope.Thread)
    public static class VarcharDirectWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(randomAsciiString(random));
                }
                else {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class VarcharNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Strings.repeat("0", 4));
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class VarcharWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(VARCHAR);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(null);
                }
            }
            return values.iterator();
        }
    }

    private static List<String> createDictionary(Random random)
    {
        List<String> dictionary = new ArrayList<>();
        for (int dictionaryIndex = 0; dictionaryIndex < DICTIONARY; dictionaryIndex++) {
            dictionary.add(randomAsciiString(random));
        }
        return dictionary;
    }

    // this is not appropriate for benchmarking with compression
    private static String randomAsciiString(Random random)
    {
        char[] value = new char[random.nextInt(MAX_STRING)];
        for (int i = 0; i < value.length; i++) {
            value[i] = (char) random.nextInt(Byte.MAX_VALUE);
        }
        return new String(value);
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class TimestampNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TIMESTAMP);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<SqlTimestamp> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlTimestamp((random.nextLong()), UTC_KEY));
            }
            return values.iterator();
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class TimestampWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(TIMESTAMP);
        }

        @Override
        protected Iterator<?> createValues()
        {
            List<SqlTimestamp> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(null);
            }
            return values.iterator();
        }
    }

    static {
        try {
            // call all versions of the long stream reader to pollute the profile
            BenchmarkStreamReaders benchmark = new BenchmarkStreamReaders();
            benchmark.readLongNoNull(BigintNoNullBenchmarkData.create());
            benchmark.readLongWithNull(BigintWithNullBenchmarkData.create());
            benchmark.readIntNoNull(IntegerNoNullBenchmarkData.create());
            benchmark.readIntWithNull(IntegerWithNullBenchmarkData.create());
            benchmark.readShortNoNull(SmallintNoNullBenchmarkData.create());
            benchmark.readShortWithNull(SmallintWithNullBenchmarkData.create());
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkStreamReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
