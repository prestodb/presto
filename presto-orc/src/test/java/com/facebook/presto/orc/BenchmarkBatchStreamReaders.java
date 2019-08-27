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
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.orc.OrcEncoding.ORC;
import static com.facebook.presto.orc.OrcReader.INITIAL_BATCH_SIZE;
import static com.facebook.presto.orc.OrcTester.Format.ORC_12;
import static com.facebook.presto.orc.OrcTester.writeOrcColumnHive;
import static com.facebook.presto.orc.metadata.CompressionKind.NONE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.joda.time.DateTimeZone.UTC;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Fork(3)
@Warmup(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkBatchStreamReaders
{
    public static final DecimalType SHORT_DECIMAL_TYPE = createDecimalType(10, 5);
    public static final DecimalType LONG_DECIMAL_TYPE = createDecimalType(30, 10);
    public static final int ROWS = 10_000_000;
    public static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);

    @Benchmark
    public Object readBooleanNoNull(BooleanNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readBooleanWithNull(BooleanWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readAllNull(AllNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readByteNoNull(TinyIntNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readByteWithNull(TinyIntWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readShortDecimalNoNull(ShortDecimalNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readShortDecimalWithNull(ShortDecimalWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readLongDecimalNoNull(LongDecimalNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readLongDecimalWithNull(LongDecimalWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readDoubleNoNull(DoubleNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readDoubleWithNull(DoubleWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readFloatNoNull(FloatNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readFloatWithNull(FloatWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readLongDirectNoNull(BigintNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readLongDirectWithNull(BigintWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readSliceDictionaryNoNull(VarcharNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readSliceDictionaryWithNull(VarcharWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readTimestampNoNull(TimestampNoNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    @Benchmark
    public Object readTimestampWithNull(TimestampWithNullBenchmarkData data)
            throws Throwable
    {
        return readAllBlocks(data.createRecordReader(), data.getType());
    }

    private Object readAllBlocks(OrcBatchRecordReader recordReader, Type type)
            throws IOException
    {
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(type, 0);
            blocks.add(block);
        }
        return blocks;
    }

    private abstract static class BenchmarkData
    {
        protected final Random random = new Random(0);
        private Type type;
        private File temporaryDirectory;
        private File orcFile;

        public void setup(Type type)
                throws Exception
        {
            this.type = type;
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

        OrcBatchRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(orcFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createBatchRecordReader(
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
                "decimal(30,10)",

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
        protected final List<?> createValues()
        {
            return NULL_VALUES;
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
        protected List<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean());
            }
            return values;
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
        protected List<?> createValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean() ? random.nextBoolean() : null);
            }
            return values;
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
        protected List<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Long.valueOf(random.nextLong()).byteValue());
            }
            return values;
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
        protected List<?> createValues()
        {
            List<Byte> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Long.valueOf(random.nextLong()).byteValue());
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
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
        protected List<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), SHORT_DECIMAL_TYPE.getPrecision(), SHORT_DECIMAL_TYPE.getScale()));
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
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
        protected List<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), SHORT_DECIMAL_TYPE.getPrecision(), SHORT_DECIMAL_TYPE.getScale()));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
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
        protected List<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), LONG_DECIMAL_TYPE.getPrecision(), LONG_DECIMAL_TYPE.getScale()));
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
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
        protected List<?> createValues()
        {
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10_000_000_000L), LONG_DECIMAL_TYPE.getPrecision(), LONG_DECIMAL_TYPE.getScale()));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class DoubleNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(DOUBLE);
        }

        @Override
        protected List<?> createValues()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextDouble());
            }
            return values;
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
        protected List<?> createValues()
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
            return values;
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
        protected List<?> createValues()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextFloat());
            }
            return values;
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
        protected List<?> createValues()
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
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BigintNoNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT);
        }

        @Override
        protected List<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextLong());
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BigintWithNullBenchmarkData
            extends BenchmarkData
    {
        @Setup
        public void setup()
                throws Exception
        {
            setup(BIGINT);
        }

        @Override
        protected List<?> createValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Long.valueOf(random.nextLong()));
                }
                else {
                    values.add(null);
                }
            }
            return values;
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
        protected List<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Strings.repeat("0", 4));
            }
            return values;
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
        protected List<?> createValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Strings.repeat("0", 4));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }
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
        protected List<?> createValues()
        {
            List<SqlTimestamp> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlTimestamp((random.nextLong()), UTC_KEY));
            }
            return values;
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
        protected List<?> createValues()
        {
            List<SqlTimestamp> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlTimestamp(random.nextLong(), UTC_KEY));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }
    }

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkBatchStreamReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
