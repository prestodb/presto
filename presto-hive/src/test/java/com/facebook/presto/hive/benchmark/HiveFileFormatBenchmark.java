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
package com.facebook.presto.hive.benchmark;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.tpch.OrderColumn;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;
import io.airlift.units.DataSize;
import it.unimi.dsi.fastutil.ints.IntArrays;
import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.util.Statistics;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveTestUtils.createTestHdfsEnvironment;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.testing.FileUtils.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
import static io.airlift.tpch.TpchTable.LINE_ITEM;
import static io.airlift.tpch.TpchTable.ORDERS;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.SECONDS)
@Measurement(iterations = 50)
@Warmup(iterations = 20)
@Fork(3)
@SuppressWarnings("UseOfSystemOutOrSystemErr")
public class HiveFileFormatBenchmark
{
    private static final long MIN_DATA_SIZE = new DataSize(50, MEGABYTE).toBytes();

    static {
        HadoopNative.requireHadoopNative();
    }

    @SuppressWarnings("deprecation")
    private static final HiveClientConfig CONFIG = new HiveClientConfig()
            .setRcfileOptimizedReaderEnabled(true)
            .setParquetOptimizedReaderEnabled(true);

    private static final ConnectorSession SESSION = new TestingConnectorSession(new HiveSessionProperties(CONFIG)
            .getSessionProperties());

    private static final HdfsEnvironment HDFS_ENVIRONMENT = createTestHdfsEnvironment(CONFIG);

    @Param({
            "LINEITEM",
            "BIGINT_SEQUENTIAL",
            "BIGINT_RANDOM",
            "VARCHAR_SMALL",
            "VARCHAR_LARGE",
            "VARCHAR_DICTIONARY",
            "MAP_VARCHAR_DOUBLE",
            "LARGE_MAP_VARCHAR_DOUBLE",
            "MAP_INT_DOUBLE",
            "LARGE_MAP_INT_DOUBLE",
            "LARGE_ARRAY_VARCHAR",
    })
    private DataSet dataSet;

    @Param({
            "NONE",
            "SNAPPY",
            "GZIP",
    })
    private HiveCompressionCodec compression;

    @Param({
            "PRESTO_RCBINARY",
            "PRESTO_RCTEXT",
            "PRESTO_ORC",
            "PRESTO_DWRF",
            "PRESTO_PARQUET",
            "HIVE_RCBINARY",
            "HIVE_RCTEXT",
            "HIVE_ORC",
            "HIVE_DWRF",
            "HIVE_PARQUET",
    })
    private FileFormat fileFormat;

    private TestData data;
    private File dataFile;

    private final File targetDir = createTempDir("presto-benchmark");

    public HiveFileFormatBenchmark()
    {
    }

    public HiveFileFormatBenchmark(DataSet dataSet, HiveCompressionCodec compression, FileFormat fileFormat)
    {
        this.dataSet = dataSet;
        this.compression = compression;
        this.fileFormat = fileFormat;
    }

    @Setup
    public void setup()
            throws IOException
    {
        data = dataSet.createTestData(fileFormat);

        targetDir.mkdirs();
        dataFile = new File(targetDir, UUID.randomUUID().toString());
        writeData(dataFile);
    }

    @TearDown
    public void tearDown()
    {
        deleteRecursively(targetDir);
    }

    @SuppressWarnings("PublicField")
    @AuxCounters
    @State(Scope.Thread)
    public static class CompressionCounter
    {
        public long inputSize;
        public long outputSize;
    }

    @Benchmark
    public List<Page> read(CompressionCounter counter)
            throws IOException
    {
        if (!fileFormat.supports(data)) {
            throw new RuntimeException(fileFormat + " does not support data set " + dataSet);
        }
        List<Page> pages = new ArrayList<>(100);
        try (ConnectorPageSource pageSource = fileFormat.createFileFormatReader(
                SESSION,
                HDFS_ENVIRONMENT,
                dataFile,
                data.getColumnNames(),
                data.getColumnTypes())) {
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    page.assureLoaded();
                    pages.add(page);
                }
            }
        }
        counter.inputSize += data.getSize();
        counter.outputSize += dataFile.length();
        return pages;
    }

    @Benchmark
    public File write(CompressionCounter counter)
            throws IOException
    {
        File targetFile = new File(targetDir, UUID.randomUUID().toString());
        writeData(targetFile);
        counter.inputSize += data.getSize();
        counter.outputSize += targetFile.length();
        return targetFile;
    }

    private void writeData(File targetFile)
            throws IOException
    {
        List<Page> inputPages = data.getPages();
        try (FormatWriter formatWriter = fileFormat.createFileFormatWriter(
                SESSION,
                targetFile,
                data.getColumnNames(),
                data.getColumnTypes(),
                compression)) {
            for (Page page : inputPages) {
                formatWriter.writePage(page);
            }
        }
    }

    public enum DataSet
    {
        LINEITEM {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, LINE_ITEM, LINE_ITEM.getColumns());
            }
        },
        BIGINT_SEQUENTIAL {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.ORDER_KEY);
            }
        },
        BIGINT_RANDOM {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.CUSTOMER_KEY);
            }
        },
        VARCHAR_SMALL {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.CLERK);
            }
        },
        VARCHAR_LARGE {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.CLERK);
            }
        },
        VARCHAR_DICTIONARY {
            @Override
            public TestData createTestData(FileFormat format)
            {
                return createTpchDataSet(format, ORDERS, OrderColumn.ORDER_PRIORITY);
            }
        },
        MAP_VARCHAR_DOUBLE {
            private static final int MIN_ENTRIES = 1;
            private static final int MAX_ENTRIES = 5;

            @Override
            public TestData createTestData(FileFormat format)
            {
                Type type = new MapType(createUnboundedVarcharType(), DOUBLE);
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();

                int[] keys = new int[] {1, 2, 3, 4, 5};

                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                    BlockBuilder mapBuilder = builder.beginBlockEntry();
                    int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                    IntArrays.shuffle(keys, random);
                    for (int entryId = 0; entryId < entries; entryId++) {
                        createUnboundedVarcharType().writeSlice(mapBuilder, Slices.utf8Slice("key" + keys[entryId]));
                        DOUBLE.writeDouble(mapBuilder, random.nextDouble());
                    }
                    builder.closeEntry();

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        LARGE_MAP_VARCHAR_DOUBLE {
            private static final int MIN_ENTRIES = 5_000;
            private static final int MAX_ENTRIES = 15_000;

            @Override
            public TestData createTestData(FileFormat format)
            {
                Type type = new MapType(createUnboundedVarcharType(), DOUBLE);
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                    BlockBuilder mapBuilder = builder.beginBlockEntry();
                    int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                    for (int entryId = 0; entryId < entries; entryId++) {
                        createUnboundedVarcharType().writeSlice(mapBuilder, Slices.utf8Slice("key" + random.nextInt(10_000_000)));
                        DOUBLE.writeDouble(mapBuilder, random.nextDouble());
                    }
                    builder.closeEntry();

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        MAP_INT_DOUBLE {
            private static final int MIN_ENTRIES = 1;
            private static final int MAX_ENTRIES = 5;

            @Override
            public TestData createTestData(FileFormat format)
            {
                Type type = new MapType(INTEGER, DOUBLE);
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();

                int[] keys = new int[] {1, 2, 3, 4, 5};

                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                    BlockBuilder mapBuilder = builder.beginBlockEntry();
                    int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                    IntArrays.shuffle(keys, random);
                    for (int entryId = 0; entryId < entries; entryId++) {
                        INTEGER.writeLong(mapBuilder, keys[entryId]);
                        DOUBLE.writeDouble(mapBuilder, random.nextDouble());
                    }
                    builder.closeEntry();

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        LARGE_MAP_INT_DOUBLE {
            private static final int MIN_ENTRIES = 5_000;
            private static final int MAX_ENTRIES = 15_0000;

            @Override
            public TestData createTestData(FileFormat format)
            {
                Type type = new MapType(INTEGER, DOUBLE);
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                    BlockBuilder mapBuilder = builder.beginBlockEntry();
                    int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                    for (int entryId = 0; entryId < entries; entryId++) {
                        INTEGER.writeLong(mapBuilder, random.nextInt(10_000_000));
                        DOUBLE.writeDouble(mapBuilder, random.nextDouble());
                    }
                    builder.closeEntry();

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        },
        LARGE_ARRAY_VARCHAR {
            private static final int MIN_ENTRIES = 5_000;
            private static final int MAX_ENTRIES = 15_0000;

            @Override
            public TestData createTestData(FileFormat format)
            {
                Type type = new ArrayType(createUnboundedVarcharType());
                Random random = new Random(1234);

                PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
                ImmutableList.Builder<Page> pages = ImmutableList.builder();
                long dataSize = 0;
                while (dataSize < MIN_DATA_SIZE) {
                    pageBuilder.declarePosition();

                    BlockBuilder builder = pageBuilder.getBlockBuilder(0);
                    BlockBuilder mapBuilder = builder.beginBlockEntry();
                    int entries = nextRandomBetween(random, MIN_ENTRIES, MAX_ENTRIES);
                    for (int entryId = 0; entryId < entries; entryId++) {
                        createUnboundedVarcharType().writeSlice(mapBuilder, Slices.utf8Slice("key" + random.nextInt(10_000_000)));
                    }
                    builder.closeEntry();

                    if (pageBuilder.isFull()) {
                        Page page = pageBuilder.build();
                        pages.add(page);
                        pageBuilder.reset();
                        dataSize += page.getSizeInBytes();
                    }
                }
                return new TestData(ImmutableList.of("map"), ImmutableList.of(type), pages.build());
            }
        };

        public abstract TestData createTestData(FileFormat format);
    }

    @SafeVarargs
    private static <E extends TpchEntity> TestData createTpchDataSet(FileFormat format, TpchTable<E> tpchTable, TpchColumn<E>... columns)
    {
        return createTpchDataSet(format, tpchTable, ImmutableList.copyOf(columns));
    }

    private static <E extends TpchEntity> TestData createTpchDataSet(FileFormat format, TpchTable<E> tpchTable, List<TpchColumn<E>> columns)
    {
        List<String> columnNames = columns.stream().map(TpchColumn::getColumnName).collect(toList());
        List<Type> columnTypes = columns.stream().map(HiveFileFormatBenchmark::getColumnType)
                .map(type -> format.supportsDate() || !DATE.equals(type) ? type : createUnboundedVarcharType())
                .collect(toList());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        ImmutableList.Builder<Page> pages = ImmutableList.builder();
        long dataSize = 0;
        for (E row : tpchTable.createGenerator(10, 1, 1)) {
            pageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                TpchColumn<E> column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                switch (column.getType().getBase()) {
                    case IDENTIFIER:
                        BIGINT.writeLong(blockBuilder, column.getIdentifier(row));
                        break;
                    case INTEGER:
                        INTEGER.writeLong(blockBuilder, column.getInteger(row));
                        break;
                    case DATE:
                        if (format.supportsDate()) {
                            DATE.writeLong(blockBuilder, column.getDate(row));
                        }
                        else {
                            createUnboundedVarcharType().writeString(blockBuilder, column.getString(row));
                        }
                        break;
                    case DOUBLE:
                        DOUBLE.writeDouble(blockBuilder, column.getDouble(row));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(row)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
            if (pageBuilder.isFull()) {
                Page page = pageBuilder.build();
                pages.add(page);
                pageBuilder.reset();
                dataSize += page.getSizeInBytes();

                if (dataSize >= MIN_DATA_SIZE) {
                    break;
                }
            }
        }
        return new TestData(columnNames, columnTypes, pages.build());
    }

    static class TestData
    {
        private final List<String> columnNames;
        private final List<Type> columnTypes;

        private final List<Page> pages;

        private final int size;

        public TestData(List<String> columnNames, List<Type> columnTypes, List<Page> pages)
        {
            this.columnNames = ImmutableList.copyOf(columnNames);
            this.columnTypes = ImmutableList.copyOf(columnTypes);
            this.pages = ImmutableList.copyOf(pages);
            this.size = (int) pages.stream().mapToLong(Page::getSizeInBytes).sum();
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }

        public List<Type> getColumnTypes()
        {
            return columnTypes;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        public int getSize()
        {
            return size;
        }
    }

    private static Type getColumnType(TpchColumn<?> input)
    {
        switch (input.getType().getBase()) {
            case IDENTIFIER:
                return BIGINT;
            case INTEGER:
                return INTEGER;
            case DATE:
                return DATE;
            case DOUBLE:
                return DOUBLE;
            case VARCHAR:
                return createUnboundedVarcharType();
        }
        throw new IllegalArgumentException("Unsupported type " + input.getType());
    }

    public static void main(String[] args)
            throws Exception
    {
        Options opt = new OptionsBuilder()
                .include(".*\\." + HiveFileFormatBenchmark.class.getSimpleName() + ".*")
                .jvmArgsAppend("-Xmx4g", "-Xms4g", "-XX:+UseG1GC")
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Statistics inputSizeStats = result.getSecondaryResults().get("inputSize").getStatistics();
            Statistics outputSizeStats = result.getSecondaryResults().get("outputSize").getStatistics();
            double compressionRatio = 1.0 * inputSizeStats.getSum() / outputSizeStats.getSum();
            String compression = result.getParams().getParam("compression");
            String fileFormat = result.getParams().getParam("fileFormat");
            String dataSet = result.getParams().getParam("dataSet");
            System.out.printf("  %-10s  %-30s  %-10s  %-25s  %2.2f  %10s ± %11s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
                    dataSet,
                    compression,
                    fileFormat,
                    compressionRatio,
                    toHumanReadableSpeed((long) inputSizeStats.getMean()),
                    toHumanReadableSpeed((long) inputSizeStats.getMeanErrorAt(0.999)),
                    inputSizeStats.getMeanErrorAt(0.999) * 100 / inputSizeStats.getMean(),
                    inputSizeStats.getN());
        }
        System.out.println();
    }

    private static String toHumanReadableSpeed(long bytesPerSecond)
    {
        String humanReadableSpeed;
        if (bytesPerSecond < 1024 * 10L) {
            humanReadableSpeed = format("%dB/s", bytesPerSecond);
        }
        else if (bytesPerSecond < 1024 * 1024 * 10L) {
            humanReadableSpeed = format("%.1fkB/s", bytesPerSecond / 1024.0f);
        }
        else if (bytesPerSecond < 1024 * 1024 * 1024 * 10L) {
            humanReadableSpeed = format("%.1fMB/s", bytesPerSecond / (1024.0f * 1024.0f));
        }
        else {
            humanReadableSpeed = format("%.1fGB/s", bytesPerSecond / (1024.0f * 1024.0f * 1024.0f));
        }
        return humanReadableSpeed;
    }

    private static int nextRandomBetween(Random random, int min, int max)
    {
        return min + random.nextInt(max - min);
    }
}
