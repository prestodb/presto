package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.CachingTpchDataProvider;
import com.facebook.presto.tpch.GeneratingTpchDataProvider;
import com.facebook.presto.tpch.MetricRecordingTpchBlocksProvider;
import com.facebook.presto.tpch.StatsTpchBlocksProvider;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchDataProvider;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.tpch.TpchSchema.columnHandle;
import static com.facebook.presto.tpch.TpchSchema.tableHandle;
import static com.google.common.base.Preconditions.checkState;

/**
 * Abstract template for benchmarks that want to test the performance of an Operator.
 */
public abstract class AbstractOperatorBenchmark
        extends AbstractBenchmark
{
    public static BlockIterable getBlockIterable(TpchBlocksProvider blocksProvider, String tableName, String columnName, BlocksFileEncoding columnEncoding)
    {
        TpchTableHandle tableHandle = tableHandle(tableName);
        TpchColumnHandle columnHandle = columnHandle(tableHandle, columnName);
        return blocksProvider.getBlocks(tableHandle, columnHandle, columnEncoding);
    }

    private static final TpchDataProvider TPCH_DATA_PROVIDER = new CachingTpchDataProvider(new GeneratingTpchDataProvider());

    protected AbstractOperatorBenchmark(String benchmarkName, int warmupIterations, int measuredIterations)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
    }

    @Override
    protected String getDefaultResult()
    {
        return "input_rows_per_second";
    }

    protected abstract Operator createBenchmarkedOperator(TpchBlocksProvider blocksProvider);

    protected long execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }
        }
        return outputRows;
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        long start = System.nanoTime();

        StatsTpchBlocksProvider statsTpchBlocksProvider = new StatsTpchBlocksProvider(TPCH_DATA_PROVIDER);
        MetricRecordingTpchBlocksProvider metricRecordingTpchBlocksProvider = new MetricRecordingTpchBlocksProvider(statsTpchBlocksProvider);

        long outputRows = execute(metricRecordingTpchBlocksProvider);

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
