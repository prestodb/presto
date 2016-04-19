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
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.HiveSessionProperties;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import io.airlift.tpch.LineItem;
import io.airlift.tpch.LineItemColumn;
import io.airlift.tpch.LineItemGenerator;
import io.airlift.tpch.TpchColumn;
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
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.spi.type.VarcharType.createUnboundedVarcharType;
import static io.airlift.testing.FileUtils.createTempDir;
import static io.airlift.testing.FileUtils.deleteRecursively;
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
    static {
        HadoopNative.requireHadoopNative();
    }

    @SuppressWarnings("deprecation")
    public static final ConnectorSession SESSION = new TestingConnectorSession(new HiveSessionProperties(
            new HiveClientConfig()
                    .setParquetOptimizedReaderEnabled(true))
            .getSessionProperties());

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

    private final File targetDir = createTempDir("presto-benchmark");
    private List<String> columnNames;
    private List<Type> columnTypes;
    private List<Type> noDateColumnTypes;

    private Page[] pages;
    private Page[] noDatePages;
    private File dataFile;
    private int size;

    @Setup
    public void setup()
            throws IOException
    {
        List<LineItemColumn> columns = ImmutableList.copyOf(LineItemColumn.values());

        columnNames = columns.stream().map(LineItemColumn::getColumnName).collect(toList());
        columnTypes = columns.stream().map(HiveFileFormatBenchmark::getColumnType).collect(toList());
        noDateColumnTypes = columnTypes.stream()
                .map(type -> DateType.DATE.equals(type) ? createUnboundedVarcharType() : type)
                .collect(toList());
        columns.stream().map(HiveFileFormatBenchmark::getColumnType).collect(toList());
        PageBuilder pageBuilder = new PageBuilder(columnTypes);
        PageBuilder noDatePageBuilder = new PageBuilder(noDateColumnTypes);

        for (LineItem lineItem : new LineItemGenerator(0.01, 1, 1)) {
            pageBuilder.declarePosition();
            noDatePageBuilder.declarePosition();
            for (int i = 0; i < columns.size(); i++) {
                LineItemColumn column = columns.get(i);
                BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                BlockBuilder noDateBlockBuilder = noDatePageBuilder.getBlockBuilder(i);
                switch (column.getType()) {
                    case IDENTIFIER:
                        BigintType.BIGINT.writeLong(blockBuilder, column.getIdentifier(lineItem));
                        BigintType.BIGINT.writeLong(noDateBlockBuilder, column.getIdentifier(lineItem));
                        break;
                    case INTEGER:
                        IntegerType.INTEGER.writeLong(blockBuilder, column.getIdentifier(lineItem));
                        IntegerType.INTEGER.writeLong(noDateBlockBuilder, column.getIdentifier(lineItem));
                        break;
                    case DATE:
                        DateType.DATE.writeLong(blockBuilder, column.getDate(lineItem));
                        createUnboundedVarcharType().writeString(noDateBlockBuilder, column.getString(lineItem));
                        break;
                    case DOUBLE:
                        DoubleType.DOUBLE.writeDouble(blockBuilder, column.getDouble(lineItem));
                        DoubleType.DOUBLE.writeDouble(noDateBlockBuilder, column.getDouble(lineItem));
                        break;
                    case VARCHAR:
                        createUnboundedVarcharType().writeSlice(blockBuilder, Slices.utf8Slice(column.getString(lineItem)));
                        createUnboundedVarcharType().writeSlice(noDateBlockBuilder, Slices.utf8Slice(column.getString(lineItem)));
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + column.getType());
                }
            }
        }
        Page page = pageBuilder.build();
        pages = new Page[] {page};
        noDatePages = new Page[] {noDatePageBuilder.build()};
        size = (int) page.getSizeInBytes();
        targetDir.mkdirs();

        dataFile = new File(targetDir, UUID.randomUUID().toString());
        writeData(dataFile);
    }

    @TearDown
    public void tearDown()
    {
        deleteRecursively(targetDir);
    }

    @Benchmark
    public List<Page> read(CompressionCounter counter)
            throws IOException
    {
        List<Page> pages = new ArrayList<>(100);
        try (ConnectorPageSource pageSource = fileFormat.createFileFormatReader(
                SESSION,
                dataFile,
                columnNames,
                fileFormat.supportsDate() ? columnTypes : noDateColumnTypes)) {
            while (!pageSource.isFinished()) {
                Page page = pageSource.getNextPage();
                if (page != null) {
                    page.assureLoaded();
                    pages.add(page);
                }
            }
        }
        counter.addCompressed(size, dataFile.length());
        return pages;
    }

    @Benchmark
    public File write(CompressionCounter counter)
            throws IOException
    {
        File targetFile = new File(targetDir, UUID.randomUUID().toString());
        writeData(targetFile);
        counter.addCompressed(size, targetFile.length());
        return targetFile;
    }

    private void writeData(File targetFile)
            throws IOException
    {
        Page[] inputPages = fileFormat.supportsDate() ? pages : noDatePages;
        try (FormatWriter formatWriter = fileFormat.createFileFormatWriter(
                SESSION,
                targetFile,
                columnNames,
                fileFormat.supportsDate() ? columnTypes : noDateColumnTypes,
                compression
        )) {
            for (Page page : inputPages) {
                formatWriter.writePage(page);
            }
        }
    }

    private static Type getColumnType(TpchColumn<?> input)
    {
        switch (input.getType()) {
            case IDENTIFIER:
                return BigintType.BIGINT;
            case INTEGER:
                return IntegerType.INTEGER;
            case DATE:
                return DateType.DATE;
            case DOUBLE:
                return DoubleType.DOUBLE;
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
                .build();

        Collection<RunResult> results = new Runner(opt).run();

        for (RunResult result : results) {
            Statistics inputSizeStats = result.getSecondaryResults().get("getInputSize").getStatistics();
            Statistics outputSizeStats = result.getSecondaryResults().get("getOutputSize").getStatistics();
            double compressionRatio = 1.0 * inputSizeStats.getSum() / outputSizeStats.getSum();
            String compression = result.getParams().getParam("compression");
            String fileFormat = result.getParams().getParam("fileFormat");
            System.out.printf("  %-10s  %-22s  %-25s  %2.2f  %10s ± %11s (%5.2f%%) (N = %d, \u03B1 = 99.9%%)\n",
                    result.getPrimaryResult().getLabel(),
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

    public static String toHumanReadableSpeed(long bytesPerSecond)
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
}
