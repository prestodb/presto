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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
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
public class BenchmarkStreamReaders
{
    public static final DecimalType DECIMAL_TYPE = createDecimalType(10, 5);
    public static final int ROWS = 10_000_000;

    @Benchmark
    public Object readBooleanNoNull(BooleanNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(BOOLEAN, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readBooleanWithNull(BooleanWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(BOOLEAN, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readByteNoNull(TinyIntNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(TINYINT, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readByteWithNull(TinyIntWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(TINYINT, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readDecimalNoNull(DecimalNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(DECIMAL_TYPE, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readDecimalWithNull(DecimalWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(DECIMAL_TYPE, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readDoubleNoNull(DoubleNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(DOUBLE, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readDoubleWithNull(DoubleWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(DOUBLE, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readFloatNoNull(FloatNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(REAL, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readFloatWithNull(FloatWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(REAL, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readLongDirectNoNull(BigintNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(BIGINT, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readLongDirectWithNull(BigintWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(BIGINT, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readSliceDictionaryNoNull(VarcharNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(VARCHAR, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readSliceDictionaryWithNull(VarcharWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(VARCHAR, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readTimestampNoNull(TimestampNoNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(TIMESTAMP, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @Benchmark
    public Object readTimestampWithNull(TimestampWithNullBenchmarkData data)
            throws Throwable
    {
        OrcRecordReader recordReader = data.createRecordReader();
        List<Block> blocks = new ArrayList<>();
        while (recordReader.nextBatch() > 0) {
            Block block = recordReader.readBlock(TIMESTAMP, 0);
            blocks.add(block);
        }
        return blocks;
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BooleanNoNullBenchmarkData
    {
        protected File temporaryDirectory;
        protected File booleanNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            booleanNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(booleanNoNullFile, ORC_12, NONE, BOOLEAN, createBooleanValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(booleanNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, BOOLEAN),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Boolean> createBooleanValuesNoNull()
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
    {
        protected File temporaryDirectory;
        protected File booleanWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            booleanWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(booleanWithNullFile, ORC_12, NONE, BOOLEAN, createBooleanValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(booleanWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, BOOLEAN),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Boolean> createBooleanValuesWithNull()
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
    {
        protected File temporaryDirectory;
        protected File tinyIntNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            tinyIntNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(tinyIntNoNullFile, ORC_12, NONE, TINYINT, createTinyIntValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(tinyIntNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, TINYINT),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Byte> createTinyIntValuesNoNull()
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
    {
        protected File temporaryDirectory;
        protected File tinyIntWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            tinyIntWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(tinyIntWithNullFile, ORC_12, NONE, TINYINT, createTinyIntValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(tinyIntWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, TINYINT),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Byte> createTinyIntValuesWithNull()
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
    public static class DecimalNoNullBenchmarkData
    {
        protected File temporaryDirectory;
        protected File decimalNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            decimalNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(decimalNoNullFile, ORC_12, NONE, DECIMAL_TYPE, createDecimalValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(decimalNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, DECIMAL_TYPE),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<SqlDecimal> createDecimalValuesNoNull()
        {
            Random random = new Random();
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10000000000L), 10, 5));
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class DecimalWithNullBenchmarkData
    {
        protected File temporaryDirectory;
        protected File decimalWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            decimalWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(decimalWithNullFile, ORC_12, NONE, DECIMAL_TYPE, createDecimalValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(decimalWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, DECIMAL_TYPE),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<SqlDecimal> createDecimalValuesWithNull()
        {
            Random random = new Random();
            List<SqlDecimal> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(new SqlDecimal(BigInteger.valueOf(random.nextLong() % 10000000000L), 10, 5));
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
    {
        protected File temporaryDirectory;
        protected File doubleNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            doubleNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(doubleNoNullFile, ORC_12, NONE, DOUBLE, createDoubleValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(doubleNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, DOUBLE),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Double> createDoubleValuesNoNull()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Double.valueOf(random.nextDouble()));
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class DoubleWithNullBenchmarkData
    {
        protected File temporaryDirectory;
        protected File doubleWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            doubleWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(doubleWithNullFile, ORC_12, NONE, DOUBLE, createDoubleValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(doubleWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, DOUBLE),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Double> createDoubleValuesWithNull()
        {
            List<Double> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Double.valueOf(random.nextDouble()));
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
    {
        protected File temporaryDirectory;
        protected File floatNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            floatNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(floatNoNullFile, ORC_12, NONE, REAL, createFloatValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(floatNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, REAL),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Float> createFloatValuesNoNull()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Float.valueOf(random.nextFloat()));
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class FloatWithNullBenchmarkData
    {
        protected File temporaryDirectory;
        protected File floatWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            floatWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(floatWithNullFile, ORC_12, NONE, REAL, createFloatValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(floatWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, REAL),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Float> createFloatValuesWithNull()
        {
            List<Float> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Float.valueOf(random.nextFloat()));
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
    {
        protected File temporaryDirectory;
        protected File bigintNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();

            bigintNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(bigintNoNullFile, ORC_12, NONE, BIGINT, createBigintValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(bigintNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, BIGINT),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Long> createBigintValuesNoNull()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Long.valueOf(random.nextLong()));
            }
            return values;
        }
    }

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Thread)
    public static class BigintWithNullBenchmarkData
    {
        protected File temporaryDirectory;
        protected File bigintWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            bigintWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(bigintWithNullFile, ORC_12, NONE, BIGINT, createBigintValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(bigintWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, BIGINT),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<Long> createBigintValuesWithNull()
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
    {
        protected File temporaryDirectory;
        protected File varcharNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            varcharNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(varcharNoNullFile, ORC_12, NONE, VARCHAR, createVarcharValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(varcharNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, VARCHAR),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<String> createVarcharValuesNoNull()
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
    {
        protected File temporaryDirectory;
        protected File varcharWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            varcharWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(varcharWithNullFile, ORC_12, NONE, VARCHAR, createVarcharValuesWithNulls().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(varcharWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, VARCHAR),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<String> createVarcharValuesWithNulls()
        {
            Random random = new Random();
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
    {
        protected File temporaryDirectory;
        protected File timestampNoNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();

            timestampNoNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(timestampNoNullFile, ORC_12, NONE, TIMESTAMP, createSqlTimeStampValuesNoNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(timestampNoNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, TIMESTAMP),
                    OrcPredicate.TRUE,
                    UTC, // arbitrary
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<SqlTimestamp> createSqlTimeStampValuesNoNull()
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
    {
        protected File temporaryDirectory;
        protected File timestampWithNullFile;
        private Random random;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();

            timestampWithNullFile = new File(temporaryDirectory, randomUUID().toString());
            writeOrcColumnHive(timestampWithNullFile, ORC_12, NONE, TIMESTAMP, createSqlTimestampValuesWithNull().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        private OrcRecordReader createRecordReader()
                throws IOException
        {
            OrcDataSource dataSource = new FileOrcDataSource(timestampWithNullFile, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), true);
            OrcReader orcReader = new OrcReader(dataSource, ORC, new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE), new DataSize(1, MEGABYTE));
            return orcReader.createRecordReader(
                    ImmutableMap.of(0, TIMESTAMP),
                    OrcPredicate.TRUE,
                    UTC,
                    newSimpleAggregatedMemoryContext(),
                    INITIAL_BATCH_SIZE);
        }

        private List<SqlTimestamp> createSqlTimestampValuesWithNull()
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
                .include(".*" + BenchmarkStreamReaders.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }
}
