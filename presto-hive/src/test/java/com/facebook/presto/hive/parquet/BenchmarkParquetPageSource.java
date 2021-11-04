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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.parquet.Field;
import com.facebook.presto.parquet.FileParquetDataSource;
import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.reader.ParquetReader;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.testing.TestingSession;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOConverter;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.schema.MessageType;
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

import javax.annotation.concurrent.Immutable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.hive.parquet.AbstractTestParquetReader.intToSqlTimestamp;
import static com.facebook.presto.hive.parquet.ParquetTester.writeParquetFileFromPresto;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.parquet.ParquetTypeUtils.getColumnIO;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.AND;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.io.Files.createTempDir;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Thread)
@OutputTimeUnit(MILLISECONDS)
@Fork(3)
@Warmup(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkParquetPageSource
{
    private static final int ROWS = 10_000_000;
    private static final List<?> NULL_VALUES = Collections.nCopies(ROWS, null);

    private static final boolean batchReadEnabled = true;
    private static final boolean enableVerification = false;

    @Benchmark
    public void readAllBlocksAndApplyFilters(BenchmarkData data)
            throws Exception
    {
        try (ParquetPageSource parquetPageSource = data.createParquetPageSource()) {
            while (true) {
                Page page = parquetPageSource.getNextPage();
                if (page == null) {
                    break;
                }

                data.process(page);
            }
        }
    }

    @Test
    public void verifyReadAllBlocksAndApplyFilters()
            throws Exception
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkParquetPageSource benchmarkSelectiveStreamReaders = new BenchmarkParquetPageSource();
        benchmarkSelectiveStreamReaders.readAllBlocksAndApplyFilters(data);
    }

    @State(Scope.Thread)
    public static class BenchmarkData
    {
        private static final int NO_FILTER = -1;
        private final int timestampBound = 1_000_000_000;

        private final Random random = new Random(0);
        private final Metadata metadata = createTestMetadataManager();
        private final Session testSession = TestingSession.testSessionBuilder().build();
        private final FunctionAndTypeManager functionManager = metadata.getFunctionAndTypeManager();

        private List<String> columnNames = new ArrayList<>();
        private PageProcessor pageProcessor;

        private File temporaryDirectory;
        private File parquetFile;
        private int channelCount;
        private List<Optional<RowExpression>> filters = new ArrayList<>();
        private ParquetMetadata parquetMetadata;

        @Param({
                "boolean",

                "int32",
                "int64",
                "int96",

                "varchar",
        })
        private String typeSignature = "boolean";

        @Param({
                "PARTIAL",
                "NONE",
                "ALL"
        })
        private Nulls withNulls = Nulls.PARTIAL;

        // 0 means no rows will be filtered out, 1 means all rows will be filtered out, -1 means no filter.
        // When withNulls is ALL, only -1, 0, 1 are meaningful. Other values are regarded as 1.
        // "|" is the column delimiter.
        @Param({
                "-1",
                "0",
                "0.1",
                "0.2",
                "0.3",
                "0.4",
                "0.5",
                "0.6",
                "0.7",
                "0.8",
                "0.9",
                "1",
                "0.0|-1",
                "0.1|-1",
                "0.2|-1",
                "0.3|-1",
                "0.4|-1",
                "0.5|-1",
                "0.6|-1",
                "0.7|-1",
                "0.8|-1",
                "0.9|-1",
                "1|-1",
                "0|0.5",
                "0.1|0.5",
                "0.2|0.5",
                "0.3|0.5",
                "0.4|0.5",
                "0.5|0.5",
                "0.6|0.5",
                "0.7|0.5",
                "0.8|0.5",
                "0.9|0.5",
                "1|0.5",
                "-1|-1",
                "1|1",
        })
        private String filterRateSignature = "0.1|-1";

        @Param({
                "UNCOMPRESSED",
                "SNAPPY",
                "GZIP",
                "ZSTD",
                "LZO",
                "BROTLI",
                "LZ4",
        })
        private CompressionCodecName compressionCodecName = UNCOMPRESSED;

        @Setup
        public void setup()
                throws Exception
        {
            temporaryDirectory = createTempDir();
            parquetFile = new File(temporaryDirectory, randomUUID().toString());
            Type type = getTypeFromTypeSignature();

            List<Float> filterRates = Arrays.stream(filterRateSignature.split("\\|")).map(r -> Float.parseFloat(r)).collect(toImmutableList());
            channelCount = filterRates.size();

            Iterator<?>[] values = new Iterator<?>[channelCount];
            for (int i = 0; i < channelCount; i++) {
                float filterRate = filterRates.get(i);
                Pair<Boolean, Float> filterInfoForNonNull = getFilterInfoForNonNull(filterRate);
                values[i] = createValues(filterRate).iterator();
                filters.add(getFilter(i, type, filterRate, filterInfoForNonNull.getKey(), filterInfoForNonNull.getValue()));
                columnNames.add("column" + i);
            }

            writeParquetFileFromPresto(parquetFile,
                    Collections.nCopies(channelCount, type),
                    columnNames,
                    values,
                    ROWS,
                    compressionCodecName);

            //Set up PageProcessor
            List<RowExpression> projections = getProjections(type);
            PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(metadata, 0);
            pageProcessor = new ExpressionCompiler(metadata, pageFunctionCompiler).compilePageProcessor(testSession.getSqlFunctionProperties(), filterConjunction(), projections).get();

            parquetMetadata = MetadataReader.readFooter(new FileParquetDataSource(parquetFile), parquetFile.length()).getParquetMetadata();
        }

        @TearDown
        public void tearDown()
                throws IOException
        {
            deleteRecursively(temporaryDirectory.toPath(), ALLOW_INSECURE);
        }

        ParquetPageSource createParquetPageSource()
                throws IOException
        {
            FileParquetDataSource dataSource = new FileParquetDataSource(parquetFile);
            MessageType schema = parquetMetadata.getFileMetaData().getSchema();
            MessageColumnIO messageColumnIO = getColumnIO(schema, schema);

            Type type = getTypeFromTypeSignature();
            List<Optional<Field>> fields = new ArrayList<>();
            for (int i = 0; i < channelCount; i++) {
                fields.add(ColumnIOConverter.constructField(getTypeFromTypeSignature(), messageColumnIO.getChild(i)));
            }

            ParquetReader parquetReader = new ParquetReader(messageColumnIO, parquetMetadata.getBlocks(), dataSource, newSimpleAggregatedMemoryContext(), new DataSize(16, MEGABYTE), batchReadEnabled, enableVerification);

            return new ParquetPageSource(parquetReader, Collections.nCopies(channelCount, type), fields, columnNames, new RuntimeStats());
        }

        private Optional<RowExpression> filterConjunction()
        {
            Optional<RowExpression> conjunction = Optional.empty();
            for (Optional<RowExpression> rowExpression : filters) {
                if (!rowExpression.isPresent()) {
                    continue;
                }

                if (!conjunction.isPresent()) {
                    conjunction = rowExpression;
                }
                else {
                    conjunction = Optional.of(specialForm(
                            AND,
                            BOOLEAN,
                            conjunction.get(),
                            rowExpression.get()));
                }
            }
            return conjunction;
        }

        public List<Optional<Page>> process(Page page)
        {
            return ImmutableList.copyOf(
                    pageProcessor.process(
                            null,
                            new DriverYieldSignal(),
                            newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                            page));
        }

        private Type getTypeFromTypeSignature()
        {
            switch (typeSignature) {
                case "boolean":
                    return BOOLEAN;
                case "int32":
                    return INTEGER;
                case "int64":
                    return BIGINT;
                case "int96":
                    return TIMESTAMP;
                case "varchar":
                    return VarcharType.createVarcharType(400);
                default:
                    throw new UnsupportedOperationException("Unsupported type: " + typeSignature);
            }
        }

        private List<RowExpression> getProjections(Type type)
        {
            ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
            for (int i = 0; i < channelCount; i++) {
                builder.add(new InputReferenceExpression(i, type));
            }

            return builder.build();
        }

        private Optional<RowExpression> getFilter(int columnIndex, Type type, float filterRate, boolean filterAllowNull, float selectionRateForNonNull)
        {
            if (filterRate == NO_FILTER) {
                return Optional.empty();
            }

            RowExpression filter = null;

            if (BOOLEAN.equals(type)) {
                filter = call(EQUAL.name(),
                        functionManager.resolveOperator(EQUAL, fromTypes(type, type)),
                        BOOLEAN,
                        field(columnIndex, type),
                        constant(true, type));
            }

            if (INTEGER.equals(type)) {
                filter = specialForm(AND,
                        BOOLEAN,
                        call(GREATER_THAN_OR_EQUAL.name(),
                                functionManager.resolveOperator(GREATER_THAN_OR_EQUAL, fromTypes(type, type)),
                                BOOLEAN,
                                field(columnIndex, type),
                                constant((long) (Integer.MIN_VALUE * selectionRateForNonNull), type)),
                        call(LESS_THAN_OR_EQUAL.name(),
                                functionManager.resolveOperator(LESS_THAN_OR_EQUAL, fromTypes(type, type)),
                                BOOLEAN,
                                field(columnIndex, type),
                                constant((long) (Integer.MAX_VALUE * selectionRateForNonNull), type)));
            }

            if (BIGINT.equals(type)) {
                filter = specialForm(AND,
                        BOOLEAN,
                        call(GREATER_THAN_OR_EQUAL.name(),
                                functionManager.resolveOperator(GREATER_THAN_OR_EQUAL, fromTypes(type, type)),
                                BOOLEAN,
                                field(columnIndex, type),
                                constant((long) (Long.MIN_VALUE * selectionRateForNonNull), type)),
                        call(LESS_THAN_OR_EQUAL.name(),
                                functionManager.resolveOperator(LESS_THAN_OR_EQUAL, fromTypes(type, type)),
                                BOOLEAN,
                                field(columnIndex, type),
                                constant((long) (Long.MAX_VALUE * selectionRateForNonNull), type)));
            }

            if (TIMESTAMP.equals(type)) {
                filter = call(LESS_THAN_OR_EQUAL.name(),
                        functionManager.resolveOperator(LESS_THAN_OR_EQUAL, fromTypes(type, type)),
                        BOOLEAN,
                        field(columnIndex, type),
                        constant((long) (timestampBound * selectionRateForNonNull), BIGINT));
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.VARCHAR)) {
                filter = call(LESS_THAN_OR_EQUAL.name(),
                        functionManager.resolveOperator(LESS_THAN_OR_EQUAL, fromTypes(type, type)),
                        BOOLEAN,
                        field(columnIndex, type),
                        constant(utf8Slice(String.format("%09d", (int) (999_999_999 * selectionRateForNonNull) - 1)), type));
            }

            if (filter == null) {
                throw new IllegalStateException("Cannot generate filter for columnIndex " + columnIndex + " of type: " + type);
            }

            if (filterAllowNull) {
                RowExpression columnIsNull = specialForm(IS_NULL,
                        BOOLEAN,
                        field(columnIndex, type));
                filter = specialForm(OR,
                        BOOLEAN,
                        columnIsNull,
                        filter);
            }

            return Optional.of(filter);
        }

        private List<?> createValues(float filterRate)
        {
            Type type = getTypeFromTypeSignature();
            switch (withNulls) {
                case ALL:
                    return NULL_VALUES;
                case PARTIAL:
                    // Let the null rate be 0.5 * (1 - filterRate)
                    return IntStream.range(0, ROWS).mapToObj(j -> random.nextFloat() > 0.5 * (filterRate == -1 ? 1 : 1 - filterRate) ? createValue(type, filterRate) : null).collect(toList());
                default:
                    return IntStream.range(0, ROWS).mapToObj(j -> createValue(type, filterRate)).collect(toList());
            }
        }

        private final Object createValue(Type type, float filterRate)
        {
            if (BOOLEAN.equals(type)) {
                switch (withNulls) {
                    case PARTIAL:
                        return random.nextFloat() <= (1 - filterRate) / (1 + filterRate);
                    case NONE:
                        return random.nextFloat() <= (1 - filterRate);
                    default:
                        throw new UnsupportedOperationException("Unsupported withNulls for boolean: " + withNulls);
                }
            }

            if (INTEGER.equals(type)) {
                return Integer.valueOf(random.nextInt());
            }

            if (BIGINT.equals(type)) {
                return Long.valueOf(random.nextLong());
            }

            if (TIMESTAMP.equals(type)) {
                return intToSqlTimestamp(random.nextInt(timestampBound));
            }

            if (type.getTypeSignature().getBase().equals(StandardTypes.VARCHAR)) {
                return String.format("%09d", random.nextInt(999_999_999));
            }

            throw new IllegalStateException("Unexpected value: " + type);
        }

        private Pair<Boolean, Float> getFilterInfoForNonNull(float filterRate)
        {
            switch (withNulls) {
                case NONE:
                    return new Pair<>(false, 1 - filterRate);
                case PARTIAL:
                    return new Pair<>(true, (1 - filterRate) / (1 + filterRate));
                case ALL:
                    return new Pair<>((filterRate == 0 ? true : false), 1f);
                default:
                    throw new UnsupportedOperationException("Unsupported withNulls: " + withNulls);
            }
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
                .include(".*" + BenchmarkParquetPageSource.class.getSimpleName() + ".*")
                .build();

        new Runner(options).run();
    }

    @Immutable
    private static class Pair<K, V>
    {
        private final K key;
        private final V value;

        public Pair(K key, V value)
        {
            this.key = requireNonNull(key, "key is null");
            this.value = requireNonNull(value, "value is null");
        }

        public K getKey()
        {
            return key;
        }

        public V getValue()
        {
            return value;
        }
    }
}
