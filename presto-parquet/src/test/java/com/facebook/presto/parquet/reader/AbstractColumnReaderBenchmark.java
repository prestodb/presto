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

import com.facebook.presto.parquet.ColumnReader;
import com.facebook.presto.parquet.ColumnReaderFactory;
import com.facebook.presto.parquet.DataPage;
import com.facebook.presto.parquet.DataPageV1;
import com.facebook.presto.parquet.ParquetEncoding;
import com.facebook.presto.parquet.PrimitiveField;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.parquet.column.values.ValuesWriter;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.parquet.ParquetEncoding.RLE;
import static com.facebook.presto.parquet.ParquetResultVerifierUtils.verifyColumnChunks;
import static com.facebook.presto.parquet.ParquetTypeUtils.getParquetEncoding;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.parquet.hadoop.metadata.CompressionCodecName.UNCOMPRESSED;

@State(Scope.Thread)
@OutputTimeUnit(SECONDS)
@Measurement(iterations = 10, time = 500, timeUnit = MILLISECONDS)
@Warmup(iterations = 5, time = 500, timeUnit = MILLISECONDS)
@Fork(2)
public abstract class AbstractColumnReaderBenchmark<T>
{
    // Parquet pages are usually about 1MB
    private static final int MIN_PAGE_SIZE = 1_000_000;
    private static final int OUTPUT_BUFFER_SIZE = MIN_PAGE_SIZE * 2; // Needs to be more than MIN_PAGE_SIZE
    private static final int MAX_VALUES = 1_000_000;

    private static final int DATA_GENERATION_BATCH_SIZE = 16384;
    private static final int READ_BATCH_SIZE = 4096;

    private static final boolean ENABLE_VERIFICATION = true;

    private final List<DataPage> dataPages = new ArrayList<>();
    private int dataPositions;

    protected PrimitiveField field;
    //
    @Param({
            "PLAIN", "DELTA_BYTE_ARRAY"
    })
    public ParquetEncoding parquetEncoding;

    @Param({
            "true", "false",
    })
    public boolean batchReaderEnabled;

    protected abstract PrimitiveField createPrimitiveField();

    protected abstract ValuesWriter createValuesWriter(int bufferSize);

    protected abstract T generateDataBatch(int size);

    protected abstract void writeValue(ValuesWriter writer, T batch, int index);

    protected abstract boolean getBatchReaderEnabled();

    @Setup
    public void setup()
            throws IOException
    {
        this.field = createPrimitiveField();

        ValuesWriter writer = createValuesWriter(OUTPUT_BUFFER_SIZE);
        int batchIndex = 0;
        T batch = generateDataBatch(DATA_GENERATION_BATCH_SIZE);

        while (writer.getBufferedSize() < MIN_PAGE_SIZE && dataPositions < MAX_VALUES) {
            if (batchIndex == DATA_GENERATION_BATCH_SIZE) {
                dataPages.add(createDataPage(writer, batchIndex));
                batch = generateDataBatch(DATA_GENERATION_BATCH_SIZE);
                batchIndex = 0;
            }
            writeValue(writer, batch, batchIndex++);
            dataPositions++;
        }

        if (batchIndex > 0) {
            dataPages.add(createDataPage(writer, batchIndex));
        }
    }

    @Benchmark
    public int read()
            throws IOException
    {
        ColumnReader columnReader = ColumnReaderFactory.createReader(field.getDescriptor(), getBatchReaderEnabled());
        columnReader.init(new PageReader(UNCOMPRESSED, new LinkedList<>(dataPages).listIterator(), MAX_VALUES, null, null, Optional.empty(), null, -1, -1), field, null);

        ColumnReader reader = null;
        if (ENABLE_VERIFICATION) {
            reader = ColumnReaderFactory.createReader(field.getDescriptor(), false);
            reader.init(new PageReader(UNCOMPRESSED, new LinkedList<>(dataPages).listIterator(), MAX_VALUES, null, null, Optional.empty(), null, -1, -1), field, null);
        }

        int rowsRead = 0;
        while (rowsRead < dataPositions) {
            int remaining = dataPositions - rowsRead;
            columnReader.prepareNextRead(Math.min(READ_BATCH_SIZE, remaining));
            ColumnChunk columnChunk = columnReader.readNext();
            rowsRead += columnChunk.getBlock().getPositionCount();
            if (ENABLE_VERIFICATION) {
                reader.prepareNextRead(Math.min(READ_BATCH_SIZE, remaining));
                ColumnChunk expected = reader.readNext();
                verifyColumnChunks(columnChunk, expected, false, field, null);
            }
        }
        return rowsRead;
    }

    private DataPage createDataPage(ValuesWriter writer, int valuesCount)
    {
        Slice data;
        try {
            data = Slices.wrappedBuffer(writer.getBytes().toByteArray());
            writer.reset();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        return new DataPageV1(
                data,
                valuesCount,
                data.length(),
                -1,
                null,
                RLE,
                RLE,
                getParquetEncoding(writer.getEncoding()));
    }

    protected static void run(Class<?> clazz)
            throws RunnerException
    {
        ChainedOptionsBuilder optionsBuilder = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .warmupMode(WarmupMode.BULK)
                .resultFormat(ResultFormatType.JSON)
                .result(format("%s/%s-result-%s.json", System.getProperty("java.io.tmpdir"), clazz.getSimpleName(), ISO_DATE_TIME.format(LocalDateTime.now())))
                .jvmArgsAppend("-Xmx4g", "-Xms4g")
                .include("^\\Q" + clazz.getName() + ".\\E");

        new Runner(optionsBuilder.build()).run();
    }
}
