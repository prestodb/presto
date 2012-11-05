package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.FileBlocksSerde.FileEncoding;
import com.facebook.presto.serde.FileBlocksSerde;
import com.facebook.presto.serde.FileBlocksSerde.FileBlockIterable;
import com.facebook.presto.serde.FileBlocksSerde.StatsCollector.Stats;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.tpch.CachingTpchDataProvider;
import com.facebook.presto.tpch.GeneratingTpchDataProvider;
import com.facebook.presto.tpch.MetricRecordingTpchDataProvider;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataProvider;
import com.facebook.presto.tpch.TpchSchema.Column;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * Abstract template for benchmarks that want to test the performance of an Operator.
 */
public abstract class AbstractOperatorBenchmark
        extends AbstractBenchmark
{
    private static final TpchDataProvider TPCH_DATA_PROVIDER = new CachingTpchDataProvider(new GeneratingTpchDataProvider());

    private final TpchDataProvider tpchDataProvider;

    protected AbstractOperatorBenchmark(String benchmarkName, int warmupIterations, int measuredIterations, TpchDataProvider tpchDataProvider)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
        this.tpchDataProvider = tpchDataProvider;
    }

    protected AbstractOperatorBenchmark(String benchmarkName, int warmupIterations, int measuredIterations)
    {
        this(benchmarkName, warmupIterations, measuredIterations, TPCH_DATA_PROVIDER);
    }

    @Override
    protected String getDefaultResult()
    {
        return "input_rows_per_second";
    }

    protected abstract Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider);

    @Override
    protected Map<String, Long> runOnce()
    {
        long start = System.nanoTime();

        MetricRecordingTpchDataProvider metricRecordingTpchDataProvider = new MetricRecordingTpchDataProvider(tpchDataProvider);
        StatsTpchBlocksProvider tpchBlocksProvider = new StatsTpchBlocksProvider(metricRecordingTpchDataProvider);

        Operator operator = createBenchmarkedOperator(tpchBlocksProvider);

        long outputRows = 0;
        for (Page page : operator) {
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }

        Duration totalDuration = Duration.nanosSince(start);
        Duration dataGenerationDuration = metricRecordingTpchDataProvider.getDataFetchElapsedTime();
        checkState(totalDuration.compareTo(dataGenerationDuration) >= 0, "total time should be at least as large as data generation time");

        // Compute the benchmark execution time without factoring in the time to generate the data source
        double executionMillis = totalDuration.convertTo(TimeUnit.MILLISECONDS) - dataGenerationDuration.toMillis();
        double executionSeconds = executionMillis / TimeUnit.SECONDS.toMillis(1);

        DataSize totalDataSize = metricRecordingTpchDataProvider.getCumulativeDataSize();
        
        checkState(!tpchBlocksProvider.getStats().isEmpty(), "no columns were fetched");
        // Use the first column fetched as the indicator of the number of rows
        long inputRows = tpchBlocksProvider.getStats().get(0).getRowCount();

        return ImmutableMap.<String, Long>builder()
                .put("elapsed_millis", (long) executionMillis)
                .put("input_rows", inputRows)
                .put("input_rows_per_second", (long) (inputRows / executionSeconds))
                .put("output_rows", outputRows)
                .put("output_rows_per_second", (long) (outputRows / executionSeconds))
                .put("input_megabytes", (long) totalDataSize.getValue(DataSize.Unit.MEGABYTE))
                .put("input_megabytes_per_second", (long) (totalDataSize.getValue(DataSize.Unit.MEGABYTE) / executionSeconds))
                .build();
    }

    private static class StatsTpchBlocksProvider
            implements TpchBlocksProvider
    {
        private final TpchDataProvider tpchDataProvider;
        private final ImmutableList.Builder<Stats> statsBuilder = ImmutableList.builder();

        private StatsTpchBlocksProvider(TpchDataProvider tpchDataProvider)
        {
            this.tpchDataProvider = checkNotNull(tpchDataProvider, "tpchDataProvider is null");
        }

        @Override
        public BlockIterable getBlocks(Column column, FileEncoding encoding)
        {
            checkNotNull(column, "column is null");
            checkNotNull(encoding, "encoding is null");
            try {
                File columnFile = tpchDataProvider.getColumnFile(column, encoding);
                Slice slice = Slices.mapFileReadOnly(columnFile);
                FileBlockIterable blocks = FileBlocksSerde.readBlocks(slice);
                statsBuilder.add(blocks.getStats());
                return blocks;
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        public List<Stats> getStats()
        {
            return statsBuilder.build();
        }
    }
}
