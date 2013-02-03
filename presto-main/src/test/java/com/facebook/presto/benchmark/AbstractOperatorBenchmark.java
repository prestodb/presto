package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
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
import com.facebook.presto.util.CpuTimer;
import com.facebook.presto.util.CpuTimer.CpuDuration;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.Map;

import static com.facebook.presto.tpch.TpchSchema.columnHandle;
import static com.facebook.presto.tpch.TpchSchema.tableHandle;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

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

    protected long[] execute(TpchBlocksProvider blocksProvider)
    {
        Operator operator = createBenchmarkedOperator(blocksProvider);

        long outputRows = 0;
        long outputBytes = 0;
        PageIterator iterator = operator.iterator(new OperatorStats());
        while (iterator.hasNext()) {
            Page page = iterator.next();
            BlockCursor cursor = page.getBlock(0).cursor();
            while (cursor.advanceNextPosition()) {
                outputRows++;
            }

            for (Block block : page.getBlocks()) {
                outputBytes += block.getDataSize().toBytes();
            }
        }
        return new long[] {outputRows, outputBytes};
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        CpuTimer cpuTimer = new CpuTimer();

        StatsTpchBlocksProvider statsTpchBlocksProvider = new StatsTpchBlocksProvider(TPCH_DATA_PROVIDER);
        MetricRecordingTpchBlocksProvider metricRecordingTpchBlocksProvider = new MetricRecordingTpchBlocksProvider(statsTpchBlocksProvider);

        long[] outputData = execute(metricRecordingTpchBlocksProvider);
        long outputRows = outputData[0];
        long outputBytes = outputData[1];

        CpuDuration totalTime = cpuTimer.elapsedTime();
        CpuDuration dataGenerationCpuDuration = metricRecordingTpchBlocksProvider.getDataFetchCpuDuration();
        checkState(totalTime.getWall().compareTo(dataGenerationCpuDuration.getWall()) >= 0, "total time should be at least as large as data generation time");
        checkState(!statsTpchBlocksProvider.getStats().isEmpty(), "no columns were fetched");

        // Compute the benchmark execution time without factoring in the time to generate the data source
        CpuDuration executionTime = totalTime.subtract(dataGenerationCpuDuration);

        DataSize inputSize = metricRecordingTpchBlocksProvider.getCumulativeDataSize();

        // Use the first column fetched as the indicator of the number of rows
        long inputRows = statsTpchBlocksProvider.getStats().get(0).getRowCount();

        return ImmutableMap.<String, Long>builder()
                // legacy computed values
                .put("elapsed_millis", (long) executionTime.getWall().toMillis())
                .put("input_rows_per_second", (long) (inputRows / executionTime.getWall().convertTo(SECONDS)))
                .put("output_rows_per_second", (long) (outputRows / executionTime.getWall().convertTo(SECONDS)))
                .put("input_megabytes", (long) inputSize.getValue(MEGABYTE))
                .put("input_megabytes_per_second", (long) (inputSize.getValue(MEGABYTE) / executionTime.getWall().convertTo(SECONDS)))

                .put("wall_nanos", (long) executionTime.getWall().convertTo(NANOSECONDS))
                .put("cpu_nanos", (long) executionTime.getCpu().convertTo(NANOSECONDS))
                .put("user_nanos", (long) executionTime.getUser().convertTo(NANOSECONDS))
                .put("input_rows", inputRows)
                .put("input_bytes", inputSize.toBytes())
                .put("output_rows", outputRows)
                .put("output_bytes", outputBytes)

                .build();
    }
}
