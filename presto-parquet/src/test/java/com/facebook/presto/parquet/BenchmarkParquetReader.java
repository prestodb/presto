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
package com.facebook.presto.parquet;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.google.common.base.Strings;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOConverter;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(3)
@Warmup(iterations = 30, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 20, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
@OperationsPerInvocation(BenchmarkParquetReader.ROWS)
public class BenchmarkParquetReader
{
    public static final int ROWS = 10_000_000;

    private static final boolean enableOptimizedReader = true;
    private static final boolean enableVerification = false;

    public static void main(String[] args)
            throws Throwable
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkParquetReader.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    private static Object read(BenchmarkData data)
            throws Exception
    {
        try (ParquetReader recordReader = data.createRecordReader()) {
            List<Block> blocks = new ArrayList<>();
            while (recordReader.nextBatch() > 0) {
                Block block = recordReader.readBlock(data.getField());
                blocks.add(block);
            }
            return blocks;
        }
    }

    @Benchmark
    public Object readBooleanNoNull(BooleanNoNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readBooleanWithNull(BooleanWithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readInt32NoNull(Int32NoNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readInt32WithNull(Int32WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readInt64NoNull(Int64NoNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readInt64WithNull(Int64WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readInt96NoNull(Int96NoNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readInt96WithNull(Int96WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readSliceDictionaryNoNull(VarcharNoNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readSliceDictionaryWithNull(VarcharWithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readListBooleanWithNull(ListBooleanWithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readListInt32WithNull(ListInt32WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readListInt64WithNull(ListInt64WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readListInt96WithNull(ListInt96WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readListSliceDictionaryWithNull(ListVarcharWithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readStructBooleanWithNull(StructBooleanWithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readStructInt32WithNull(StructInt32WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readStructInt64WithNull(StructInt64WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readStructInt96WithNull(StructInt96WithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    @Benchmark
    public Object readStructSliceDictionaryWithNull(StructVarcharWithNullBenchmarkData data)
            throws Throwable
    {
        return read(data);
    }

    private abstract static class BenchmarkData
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
            ParquetTestUtils.writeParquetColumnHive(file, "column", getNullability(), getType(), generateValues().iterator());
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        ParquetReader createRecordReader()
                throws IOException
        {
            FileParquetDataSource dataSource = new FileParquetDataSource(file);
            ParquetMetadata parquetMetadata = MetadataReader.readFooter(dataSource, file.length()).getParquetMetadata();
            MessageType schema = parquetMetadata.getFileMetaData().getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(schema, schema);

            this.field = ColumnIOConverter.constructField(getType(), messageColumnIO.getChild(0)).get();

            return new ParquetReader(messageColumnIO, parquetMetadata.getBlocks(), dataSource, newSimpleAggregatedMemoryContext(), new DataSize(16, MEGABYTE), enableOptimizedReader, enableVerification);
        }

        protected boolean getNullability()
        {
            return true;
        }

        Field getField()
        {
            return field;
        }

        protected abstract List<?> generateValues();

        protected abstract Type getType();
    }

    @State(Scope.Thread)
    public static class BooleanNoNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected Type getType()
        {
            return BOOLEAN;
        }

        @Override
        protected List<Boolean> generateValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean());
            }
            return values;
        }

        @Override
        protected boolean getNullability()
        {
            return false;
        }
    }

    @State(Scope.Thread)
    public static class BooleanWithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<Boolean> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(random.nextBoolean() ? random.nextBoolean() : null);
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return BOOLEAN;
        }
    }

    @State(Scope.Thread)
    public static class Int32NoNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Integer.valueOf(random.nextInt()));
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return INTEGER;
        }

        @Override
        protected boolean getNullability()
        {
            return false;
        }
    }

    @State(Scope.Thread)
    public static class Int32WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<Integer> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Integer.valueOf(random.nextInt()));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return INTEGER;
        }
    }

    @State(Scope.Thread)
    public static class Int64NoNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Long.valueOf(random.nextLong()));
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return BIGINT;
        }

        @Override
        protected boolean getNullability()
        {
            return false;
        }
    }

    @State(Scope.Thread)
    public static class Int64WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
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

        @Override
        protected Type getType()
        {
            return BIGINT;
        }
    }

    @State(Scope.Thread)
    public static class Int96NoNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Long.valueOf(random.nextInt(1572281176) * 1000));
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return TIMESTAMP;
        }

        @Override
        protected boolean getNullability()
        {
            return false;
        }
    }

    @State(Scope.Thread)
    public static class Int96WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<Long> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Long.valueOf(random.nextInt(1572281176) * 1000));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return TIMESTAMP;
        }
    }

    @State(Scope.Thread)
    public static class VarcharNoNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<String> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Strings.repeat("0", 4));
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return VarcharType.createVarcharType(400);
        }

        @Override
        protected boolean getNullability()
        {
            return false;
        }
    }

    @State(Scope.Thread)
    public static class VarcharWithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
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

        @Override
        protected Type getType()
        {
            return VarcharType.createVarcharType(400);
        }
    }

    @State(Scope.Thread)
    public static class StructBooleanWithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<List<Boolean>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                values.add(Collections.singletonList(random.nextBoolean() ? random.nextBoolean() : null));
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            RowType.Field field = RowType.field("struct", BOOLEAN);
            return RowType.from(Collections.singletonList(field));
        }
    }

    @State(Scope.Thread)
    public static class StructInt32WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<List<Integer>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Collections.singletonList(Integer.valueOf(random.nextInt())));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            RowType.Field field = RowType.field("struct", INTEGER);
            return RowType.from(Collections.singletonList(field));
        }
    }

    @State(Scope.Thread)
    public static class StructInt64WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<List<Long>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Collections.singletonList(Long.valueOf(random.nextLong())));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            RowType.Field field = RowType.field("struct", BIGINT);
            return RowType.from(Collections.singletonList(field));
        }
    }

    @State(Scope.Thread)
    public static class StructInt96WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            List<List<Long>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Collections.singletonList(Long.valueOf(random.nextInt(1572281176) * 1000)));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            RowType.Field field = RowType.field("struct", TIMESTAMP);
            return RowType.from(Collections.singletonList(field));
        }
    }

    @State(Scope.Thread)
    public static class StructVarcharWithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
            List<List<String>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(Collections.singletonList(Strings.repeat("0", 4)));
                }
                else {
                    values.add(null);
                }
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            RowType.Field field = RowType.field("struct", VarcharType.createVarcharType(400));
            return RowType.from(Collections.singletonList(field));
        }
    }

    @State(Scope.Thread)
    public static class ListVarcharWithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
            List<List<String>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(null);
                    continue;
                }
                int size = random.nextInt(5) + 1;
                List<String> entry = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    if (random.nextBoolean()) {
                        entry.add(Strings.repeat("0", 4));
                    }
                    else {
                        entry.add(null);
                    }
                }
                values.add(entry);
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return new ArrayType(VarcharType.createVarcharType(400));
        }
    }

    @State(Scope.Thread)
    public static class ListBooleanWithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
            List<List<Boolean>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(null);
                    continue;
                }
                int size = random.nextInt(5) + 1;
                List<Boolean> entry = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    if (random.nextBoolean()) {
                        entry.add(random.nextBoolean());
                    }
                    else {
                        entry.add(null);
                    }
                }
                values.add(entry);
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return new ArrayType(BOOLEAN);
        }
    }

    @State(Scope.Thread)
    public static class ListInt32WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
            List<List<Integer>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(null);
                    continue;
                }
                int size = random.nextInt(5) + 1;
                List<Integer> entry = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    if (random.nextBoolean()) {
                        entry.add(Integer.valueOf(random.nextInt()));
                    }
                    else {
                        entry.add(null);
                    }
                }
                values.add(entry);
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return new ArrayType(INTEGER);
        }
    }

    @State(Scope.Thread)
    public static class ListInt64WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
            List<List<Long>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(null);
                    continue;
                }
                int size = random.nextInt(5) + 1;
                List<Long> entry = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    if (random.nextBoolean()) {
                        entry.add(Long.valueOf(random.nextLong()));
                    }
                    else {
                        entry.add(null);
                    }
                }
                values.add(entry);
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return new ArrayType(BIGINT);
        }
    }

    @State(Scope.Thread)
    public static class ListInt96WithNullBenchmarkData
            extends BenchmarkData
    {
        @Override
        protected List<?> generateValues()
        {
            Random random = new Random(0);
            List<List<Long>> values = new ArrayList<>();
            for (int i = 0; i < ROWS; ++i) {
                if (random.nextBoolean()) {
                    values.add(null);
                    continue;
                }
                int size = random.nextInt(5) + 1;
                List<Long> entry = new ArrayList<>();
                for (int j = 0; j < size; j++) {
                    if (random.nextBoolean()) {
                        entry.add(Long.valueOf(random.nextInt(1572281176) * 1000));
                    }
                    else {
                        entry.add(null);
                    }
                }
                values.add(entry);
            }
            return values;
        }

        @Override
        protected Type getType()
        {
            return new ArrayType(TIMESTAMP);
        }
    }

    static {
        try {
            // call all versions of the column readers to pollute the profile
            BenchmarkParquetReader benchmark = new BenchmarkParquetReader();

            // simple types
            BooleanNoNullBenchmarkData dataBoolNoNull = new BooleanNoNullBenchmarkData();
            dataBoolNoNull.setup();
            benchmark.readBooleanNoNull(dataBoolNoNull);

            BooleanWithNullBenchmarkData dataBoolWithNull = new BooleanWithNullBenchmarkData();
            dataBoolWithNull.setup();
            benchmark.readBooleanWithNull(dataBoolWithNull);

            Int32NoNullBenchmarkData dataIntNoNull = new Int32NoNullBenchmarkData();
            dataIntNoNull.setup();
            benchmark.readInt32NoNull(dataIntNoNull);

            Int32WithNullBenchmarkData dataIntWithNull = new Int32WithNullBenchmarkData();
            dataIntWithNull.setup();
            benchmark.readInt32WithNull(dataIntWithNull);

            Int64NoNullBenchmarkData dataInt64NoNull = new Int64NoNullBenchmarkData();
            dataInt64NoNull.setup();
            benchmark.readInt64NoNull(dataInt64NoNull);

            Int64WithNullBenchmarkData dataInt64WithNull = new Int64WithNullBenchmarkData();
            dataInt64WithNull.setup();
            benchmark.readInt64WithNull(dataInt64WithNull);

            Int96NoNullBenchmarkData dataInt96NoNull = new Int96NoNullBenchmarkData();
            dataInt96NoNull.setup();
            benchmark.readInt96NoNull(dataInt96NoNull);

            Int96WithNullBenchmarkData dataInt96WithNull = new Int96WithNullBenchmarkData();
            dataInt96WithNull.setup();
            benchmark.readInt96WithNull(dataInt96WithNull);

            VarcharNoNullBenchmarkData dataVarcharNoNull = new VarcharNoNullBenchmarkData();
            dataVarcharNoNull.setup();
            benchmark.readSliceDictionaryNoNull(dataVarcharNoNull);

            VarcharWithNullBenchmarkData dataVarcharWithNull = new VarcharWithNullBenchmarkData();
            dataVarcharWithNull.setup();
            benchmark.readSliceDictionaryWithNull(dataVarcharWithNull);

            // List types
            ListBooleanWithNullBenchmarkData dataListBoolWithNull = new ListBooleanWithNullBenchmarkData();
            dataListBoolWithNull.setup();
            benchmark.readListBooleanWithNull(dataListBoolWithNull);

            ListInt32WithNullBenchmarkData dataListIntWithNull = new ListInt32WithNullBenchmarkData();
            dataListIntWithNull.setup();
            benchmark.readListInt32WithNull(dataListIntWithNull);

            ListInt64WithNullBenchmarkData dataListInt64WithNull = new ListInt64WithNullBenchmarkData();
            dataListInt64WithNull.setup();
            benchmark.readListInt64WithNull(dataListInt64WithNull);

            ListInt96WithNullBenchmarkData dataListInt96WithNull = new ListInt96WithNullBenchmarkData();
            dataListInt96WithNull.setup();
            benchmark.readListInt96WithNull(dataListInt96WithNull);

            ListVarcharWithNullBenchmarkData dataListVarcharWithNull = new ListVarcharWithNullBenchmarkData();
            dataListVarcharWithNull.setup();
            benchmark.readListSliceDictionaryWithNull(dataListVarcharWithNull);

            // struct types
            StructBooleanWithNullBenchmarkData dataStructBoolWithNull = new StructBooleanWithNullBenchmarkData();
            dataStructBoolWithNull.setup();
            benchmark.readStructBooleanWithNull(dataStructBoolWithNull);

            StructInt32WithNullBenchmarkData dataStructInt32WithNull = new StructInt32WithNullBenchmarkData();
            dataStructInt32WithNull.setup();
            benchmark.readStructInt32WithNull(dataStructInt32WithNull);

            StructInt64WithNullBenchmarkData dataStructInt64WithNull = new StructInt64WithNullBenchmarkData();
            dataStructInt64WithNull.setup();
            benchmark.readStructInt64WithNull(dataStructInt64WithNull);

            StructInt96WithNullBenchmarkData dataStructInt96WithNull = new StructInt96WithNullBenchmarkData();
            dataStructInt96WithNull.setup();
            benchmark.readStructInt96WithNull(dataStructInt96WithNull);

            StructVarcharWithNullBenchmarkData dataStructVarcharWithNull = new StructVarcharWithNullBenchmarkData();
            dataStructVarcharWithNull.setup();
            benchmark.readStructSliceDictionaryWithNull(dataStructVarcharWithNull);
        }
        catch (Throwable throwable) {
            throw new RuntimeException(throwable);
        }
    }
}
