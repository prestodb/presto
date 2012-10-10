package com.facebook.presto.benchmark;

import com.facebook.presto.block.*;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.TpchDataProvider;
import com.facebook.presto.tpch.TpchSchema;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Abstract template for benchmarks that want to test the performance of a single TupleStream.
 *
 * How to use this:
 * - Subclasses must call loadColumnFile(...) in the setUp phase to specify the columns of interest.
 * - Subclasses must implement createBenchmarkedTupleStream(...) where the argument will be a list of
 * decoded TupleStreams that will occur in the same order as which loadColumnFile was originally called.
 * The output should be the TupleStream that will be benchmarked.
 * - The first column to be requested will be used to represent the count of the number of input rows
 */
public abstract class AbstractTupleStreamBenchmark
    extends AbstractBenchmark
{
    private final TpchDataProvider tpchDataProvider;
    private List<TupleStreamDataSource> dataSources = new ArrayList<>();

    protected AbstractTupleStreamBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, TpchDataProvider tpchDataProvider)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
        this.tpchDataProvider = tpchDataProvider;
    }

    protected AbstractTupleStreamBenchmark(String benchmarkName, int warmupIterations, int measuredIterations)
    {
        this(benchmarkName, warmupIterations, measuredIterations, BenchmarkSuite.TPCH_DATA_PROVIDER);
    }

    @Override
    protected String getDefaultResult()
    {
        return "input_rows_per_second";
    }

    /**
     * Loads a specific TPCH column to be used for benchmarking. Requested column order will match
     * the arguments provided to createBenchmarkedTupleStream(...)
     *
     * @param column - TPCH column
     * @param encoding - Encoding to use for this column
     */
    protected void loadColumnFile(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding)
    {
        Preconditions.checkNotNull(column, "column is null");
        Preconditions.checkNotNull(encoding, "encoding is null");
        try {
            File columnFile = tpchDataProvider.getColumnFile(column, encoding);
            dataSources.add(new TupleStreamDataSource(columnFile));
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    /**
     * Creates the TupleStream to be benchmarked given the list of previously requested columns
     * as TupleStreams (in the order they were requested).
     *
     * @param inputTupleStreams - Column decoded TupleStreams
     * @return TupleStream to be benchmarked
     */
    protected abstract TupleStream createBenchmarkedTupleStream(List<? extends TupleStream> inputTupleStreams);

    @Override
    protected Map<String, Long> runOnce()
    {
        Preconditions.checkState(!dataSources.isEmpty(), "No data sources requested!");

        long start = System.nanoTime();

        ImmutableList.Builder<StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStream> builder = ImmutableList.builder();
        for (TupleStreamDataSource dataSource : dataSources) {
            builder.add(TupleStreamSerdes.createDefaultDeserializer().deserialize(dataSource.getSlice()));
        }
        ImmutableList<StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStream> inputTupleStreams = builder.build();

        TupleStream tupleStream = createBenchmarkedTupleStream(inputTupleStreams);

        Cursor cursor = tupleStream.cursor(new QuerySession());
        long outputRows = 0;
        while (Cursors.advanceNextValueNoYield(cursor)) {
            outputRows += cursor.getCurrentValueEndPosition() - cursor.getPosition() + 1;
        }

        Duration duration = Duration.nanosSince(start);

        long elapsedMillis = (long) duration.convertTo(TimeUnit.MILLISECONDS);
        double elapsedSeconds = duration.convertTo(TimeUnit.SECONDS);

        double inputBytes = 0;
        for (TupleStreamDataSource dataSource : dataSources) {
            inputBytes += dataSource.getFile().length();
        }
        DataSize totalDataSize = new DataSize(inputBytes, DataSize.Unit.BYTE);

        return ImmutableMap.<String, Long>builder()
                .put("elapsed_millis", elapsedMillis)
                .put("input_rows", inputTupleStreams.get(0).getStats().getRowCount())
                .put("input_rows_per_second", (long) (inputTupleStreams.get(0).getStats().getRowCount() / elapsedSeconds))
                .put("output_rows", outputRows)
                .put("output_rows_per_second", (long) (outputRows / elapsedSeconds))
                .put("input_megabytes", (long) totalDataSize.getValue(DataSize.Unit.MEGABYTE))
                .put("input_megabytes_per_second", (long) (totalDataSize.getValue(DataSize.Unit.MEGABYTE) / elapsedSeconds))
                .build();
    }

    private static class TupleStreamDataSource
    {
        private final File file;
        private final Slice slice;

        private TupleStreamDataSource(File file)
        {
            this.file = file;
            try {
                this.slice = Slices.mapFileReadOnly(file);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public File getFile()
        {
            return file;
        }

        public Slice getSlice()
        {
            return slice;
        }
    }
}
