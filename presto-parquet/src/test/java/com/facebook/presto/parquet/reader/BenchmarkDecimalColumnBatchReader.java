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
package com.facebook.presto.parquet.reader;

import com.facebook.airlift.units.DataSize;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.parquet.BenchmarkParquetReader;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties.WriterVersion;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOConverter;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type.Repetition;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.BenchmarkParquetReader.ROWS;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.parquet.reader.TestData.longToBytes;
import static com.facebook.presto.parquet.reader.TestData.maxPrecision;
import static com.facebook.presto.parquet.reader.TestData.unscaledRandomShortDecimalSupplier;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE;
import static org.apache.parquet.schema.MessageTypeParser.parseMessageType;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkParquetReader.ROWS)
public class BenchmarkDecimalColumnBatchReader
{
    public static final int DICT_PAGE_SIZE = 512;
    public static final String FIELD_NAME = "decimal_test_column";

    @Param({
            "true", "false",
    })
    public boolean enableOptimizedReader;

    @Param({
            "true", "false",
    })
    public static boolean nullable = true;

    @Param({
            "PARQUET_1_0", "PARQUET_2_0",
    })
    // PARQUET_1_0 => PLAIN
    // PARQUET_2_0 => DELTA_BYTE_ARRAY, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY
    public static WriterVersion writerVersion = PARQUET_1_0;

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkDecimalColumnBatchReader.class.getSimpleName() + ".*")
                .resultFormat(ResultFormatType.JSON)
                .result(format("%s/%s-result-%s.json", System.getProperty("java.io.tmpdir"), BenchmarkDecimalColumnBatchReader.class.getSimpleName(), ISO_DATE_TIME.format(LocalDateTime.now())))
                .shouldFailOnError(true)
                .build();

        new Runner(options).run();
    }

    @Benchmark
    public Object readShortDecimalByteArrayLength(ShortDecimalByteArrayLengthBenchmarkData data)
            throws Throwable
    {
        return read(data, enableOptimizedReader);
    }

    @Benchmark
    public Object readShortDecimal(ShortDecimalBenchmarkData data)
            throws Throwable
    {
        return read(data, enableOptimizedReader);
    }

    @Benchmark
    public Object readLongDecimal(LongDecimalBenchmarkData data)
            throws Throwable
    {
        return read(data, enableOptimizedReader);
    }

    public static Object read(BenchmarkData data, boolean enableOptimizedReader)
            throws Exception
    {
        try (ParquetReader recordReader = data.createRecordReader(enableOptimizedReader)) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(data.field);
                blocks.add(block);
            }
            return blocks;
        }
    }

    @State(Scope.Thread)
    public static class ShortDecimalByteArrayLengthBenchmarkData
            extends BenchmarkData
    {
        @Param({
                "1", "2", "3", "4", "5", "6", "7", "8",
        })
        public int byteArrayLength = 1;

        @Override
        protected Type getType()
        {
            return DecimalType.createDecimalType(getPrecision(), getScale());
        }

        @Override
        protected String getPrimitiveTypeName()
        {
            return "FIXED_LEN_BYTE_ARRAY(" + byteArrayLength + ")";
        }

        @Override
        protected int getPrecision()
        {
            return maxPrecision(byteArrayLength);
        }

        @Override
        protected int getScale()
        {
            return 1;
        }

        @Override
        protected MessageType getSchema()
        {
            DecimalType decimalType = (DecimalType) getType();
            String type = format("DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            return parseMessageType(
                    "message test { "
                            + Repetition.REQUIRED + " " + getPrimitiveTypeName() + " " + FIELD_NAME + " (" + type + "); "
                            + "} ");
        }

        @Override
        protected List<Object> generateValues()
        {
            List<Object> values = new ArrayList<>();
            int precision = ((DecimalType) getType()).getPrecision();
            long[] dataGen = unscaledRandomShortDecimalSupplier(byteArrayLength * Byte.SIZE, precision).apply(ROWS);

            for (int i = 0; i < ROWS; ++i) {
                values.add(Binary.fromConstantByteArray(longToBytes(dataGen[i], byteArrayLength)));
            }
            return values;
        }
    }

    @State(Scope.Thread)
    public static class ShortDecimalBenchmarkData
            extends BenchmarkData
    {
        @Param({
                "INT32", "INT64", "BINARY", "FIXED_LEN_BYTE_ARRAY(8)",
        })
        public static String decimalPrimitiveTypeName = "FIXED_LEN_BYTE_ARRAY(8)";

        @Override
        protected Type getType()
        {
            return DecimalType.createDecimalType(getPrecision(), getScale());
        }

        @Override
        protected String getPrimitiveTypeName()
        {
            return decimalPrimitiveTypeName;
        }

        @Override
        protected int getPrecision()
        {
            switch (getPrimitiveTypeName()) {
                case "INT32":
                    return 9;
                default:
                    return 18;
            }
        }

        @Override
        protected int getScale()
        {
            switch (getPrimitiveTypeName()) {
                case "INT32":
                case "INT64":
                    return 0;
                default:
                    return 12;
            }
        }

        @Override
        protected MessageType getSchema()
        {
            boolean nullability = getNullability();
            Repetition repetition = nullability ? Repetition.OPTIONAL : Repetition.REQUIRED;

            DecimalType decimalType = (DecimalType) getType();
            String type = format("DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            return parseMessageType(
                    "message test { "
                            + repetition + " " + getPrimitiveTypeName() + " " + FIELD_NAME + " (" + type + "); "
                            + "} ");
        }

        @Override
        protected List<Object> generateValues()
        {
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (getNullability()) {
                    if (random.nextBoolean()) {
                        switch (getPrimitiveTypeName()) {
                            case "INT32":
                                values.add(random.nextInt());
                                break;
                            case "INT64":
                                values.add(random.nextLong());
                                break;
                            default:
                                values.add(Binary.fromConstantByteArray(longToBytes(random.nextLong(), 8)));
                                break;
                        }
                    }
                    else {
                        values.add(null);
                    }
                }
                else {
                    switch (getPrimitiveTypeName()) {
                        case "INT32":
                            values.add(random.nextInt());
                            break;
                        case "INT64":
                            values.add(random.nextLong());
                            break;
                        default:
                            values.add(Binary.fromConstantByteArray(longToBytes(random.nextLong(), 8)));
                            break;
                    }
                }
            }
            return values;
        }

        protected boolean getNullability()
        {
            return nullable;
        }
    }

    @State(Scope.Thread)
    public static class LongDecimalBenchmarkData
            extends BenchmarkData
    {
        @Param({
                "BINARY", "FIXED_LEN_BYTE_ARRAY(16)",
        })
        public static String decimalPrimitiveTypeName = "FIXED_LEN_BYTE_ARRAY(16)";

        @Override
        protected Type getType()
        {
            return DecimalType.createDecimalType(getPrecision(), getScale());
        }

        @Override
        protected String getPrimitiveTypeName()
        {
            return decimalPrimitiveTypeName;
        }

        @Override
        protected int getPrecision()
        {
            return 38;
        }

        @Override
        protected int getScale()
        {
            return 2;
        }

        @Override
        protected MessageType getSchema()
        {
            boolean nullability = getNullability();
            Repetition repetition = nullability ? Repetition.OPTIONAL : Repetition.REQUIRED;

            DecimalType decimalType = (DecimalType) getType();
            String type = format("DECIMAL(%d,%d)", decimalType.getPrecision(), decimalType.getScale());
            return parseMessageType(
                    "message test { "
                            + repetition + " " + getPrimitiveTypeName() + " " + FIELD_NAME + " (" + type + "); "
                            + "} ");
        }

        @Override
        protected List<Binary> generateValues()
        {
            List<Binary> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (getNullability()) {
                    if (random.nextBoolean()) {
                        values.add(Binary.fromConstantByteArray(longToBytes(random.nextLong(), 16)));
                    }
                    else {
                        values.add(null);
                    }
                }
                else {
                    values.add(Binary.fromConstantByteArray(longToBytes(random.nextLong(), 16)));
                }
            }
            return values;
        }

        protected boolean getNullability()
        {
            return nullable;
        }
    }

    public abstract static class BenchmarkData
    {
        protected File temporaryDirectory;
        protected File file;
        protected Random random;

        private Field field;

        @Setup
        public void setup()
                throws Exception
        {
            random = new Random(0);
            temporaryDirectory = createTempDir();
            file = new File(temporaryDirectory, randomUUID().toString());
            generateData(new Path(file.getAbsolutePath()), getSchema(), generateValues(), getPrimitiveTypeName());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        ParquetReader createRecordReader(boolean enableOptimizedReader)
                throws IOException
        {
            FileParquetDataSource dataSource = new FileParquetDataSource(file);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, file.length(), Optional.empty(), false).getParquetMetadata();
            MessageType schema = parquetMetadata.getFileMetaData().getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(schema, schema);
            this.field = ColumnIOConverter.constructField(getType(), messageColumnIO.getChild(0)).get();

            return new ParquetReader(
                    messageColumnIO,
                    parquetMetadata.getBlocks(),
                    Optional.empty(),
                    dataSource,
                    newSimpleAggregatedMemoryContext(),
                    new DataSize(16, MEGABYTE),
                    enableOptimizedReader,
                    false,
                    null,
                    null,
                    false,
                    Optional.empty());
        }

        protected abstract List<?> generateValues();

        protected abstract MessageType getSchema();

        protected abstract String getPrimitiveTypeName();

        protected abstract Type getType();

        protected abstract int getPrecision();

        protected abstract int getScale();
    }

    public static void generateData(Path outFile, MessageType schema, List<?> dataList, String primitiveTypeName)
            throws IOException
    {
        System.out.println("Generating data @ " + outFile);

        Configuration configuration = new Configuration();
        GroupWriteSupport.setSchema(schema, configuration);
        SimpleGroupFactory f = new SimpleGroupFactory(schema);
        ParquetWriter<Group> writer = new ParquetWriter<Group>(
                outFile,
                new GroupWriteSupport(),
                CompressionCodecName.UNCOMPRESSED,
                DEFAULT_BLOCK_SIZE,
                DEFAULT_PAGE_SIZE,
                DICT_PAGE_SIZE,
                true,
                false,
                writerVersion,
                configuration);

        for (Object data : dataList) {
            if (data == null) {
                writer.write(f.newGroup());
            }
            else {
                switch (primitiveTypeName) {
                    case "INT32":
                        writer.write(f.newGroup().append(FIELD_NAME, (int) data));
                        break;
                    case "INT64":
                        writer.write(f.newGroup().append(FIELD_NAME, (long) data));
                        break;
                    default:
                        writer.write(f.newGroup().append(FIELD_NAME, (Binary) data));
                }
            }
        }
        writer.close();
    }

    static {
        try {
            BenchmarkDecimalColumnBatchReader benchmark = new BenchmarkDecimalColumnBatchReader();

            ShortDecimalByteArrayLengthBenchmarkData shortDecimalByteArrayLengthBenchmarkData = new ShortDecimalByteArrayLengthBenchmarkData();
            shortDecimalByteArrayLengthBenchmarkData.setup();
            benchmark.readShortDecimalByteArrayLength(shortDecimalByteArrayLengthBenchmarkData);

            ShortDecimalBenchmarkData dataShortDecimal = new ShortDecimalBenchmarkData();
            dataShortDecimal.setup();
            benchmark.readShortDecimal(dataShortDecimal);

            LongDecimalBenchmarkData dataLongDecimal = new LongDecimalBenchmarkData();
            dataLongDecimal.setup();
            benchmark.readLongDecimal(dataLongDecimal);
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
