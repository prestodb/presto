package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.Page;
import com.facebook.presto.tpch.CachingTpchDataProvider;
import com.facebook.presto.tpch.GeneratingTpchDataProvider;
import com.facebook.presto.tpch.MetricRecordingTpchBlocksProvider;
import com.facebook.presto.tpch.StatsTpchBlocksProvider;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataProvider;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

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

        StatsTpchBlocksProvider statsTpchBlocksProvider = new StatsTpchBlocksProvider(tpchDataProvider);
        MetricRecordingTpchBlocksProvider metricRecordingTpchBlocksProvider = new MetricRecordingTpchBlocksProvider(statsTpchBlocksProvider);

        Operator operator = createBenchmarkedOperator(metricRecordingTpchBlocksProvider);

        long outputRows = 0;
        for (Page page : operator) {
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }

        Duration totalDuration = Duration.nanosSince(start);
        Duration dataGenerationDuration = metricRecordingTpchBlocksProvider.getDataFetchElapsedTime();
        checkState(totalDuration.compareTo(dataGenerationDuration) >= 0, "total time should be at least as large as data generation time");

        // Compute the benchmark execution time without factoring in the time to generate the data source
        double totalElapsedMillis = totalDuration.convertTo(TimeUnit.MILLISECONDS);
        double dataGenerationElapsedMillis = dataGenerationDuration.toMillis();
        double executionMillis = totalElapsedMillis - dataGenerationElapsedMillis;
        double executionSeconds = executionMillis / TimeUnit.SECONDS.toMillis(1);

        DataSize totalDataSize = metricRecordingTpchBlocksProvider.getCumulativeDataSize();
        
        checkState(!statsTpchBlocksProvider.getStats().isEmpty(), "no columns were fetched");
        // Use the first column fetched as the indicator of the number of rows
        long inputRows = statsTpchBlocksProvider.getStats().get(0).getRowCount();

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

}
