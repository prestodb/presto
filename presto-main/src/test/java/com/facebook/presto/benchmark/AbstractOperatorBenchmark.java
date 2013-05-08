package com.facebook.presto.benchmark;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.tpch.CachingTpchDataFileLoader;
import com.facebook.presto.tpch.DataFileTpchBlocksProvider;
import com.facebook.presto.tpch.GeneratingTpchDataFileLoader;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.util.CpuTimer;
import com.facebook.presto.util.CpuTimer.CpuDuration;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;

import java.util.Map;

import static com.facebook.presto.tpch.TpchMetadata.TPCH_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Abstract template for benchmarks that want to test the performance of an Operator.
 */
public abstract class AbstractOperatorBenchmark
        extends AbstractBenchmark
{
    private static final TpchBlocksProvider TPCH_BLOCKS_PROVIDER = new DataFileTpchBlocksProvider(new CachingTpchDataFileLoader(new GeneratingTpchDataFileLoader()));

    protected AbstractOperatorBenchmark(String benchmarkName, int warmupIterations, int measuredIterations)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
    }

    protected TpchBlocksProvider getTpchBlocksProvider()
    {
        return TPCH_BLOCKS_PROVIDER;
    }

    protected BlockIterable getBlockIterable(String tableName, String columnName, BlocksFileEncoding columnEncoding)
    {
        ConnectorMetadata metadata = new TpchMetadata();
        TableHandle tableHandle = metadata.getTableHandle(new SchemaTableName(TPCH_SCHEMA_NAME, tableName));
        ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, columnName);
        checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
        return getTpchBlocksProvider().getBlocks((TpchTableHandle) tableHandle, (TpchColumnHandle) columnHandle, 0, 1, columnEncoding);
    }

    protected abstract Operator createBenchmarkedOperator();

    protected long[] execute(OperatorStats operatorStats)
    {
        Operator operator = createBenchmarkedOperator();

        long outputRows = 0;
        long outputBytes = 0;
        PageIterator iterator = operator.iterator(operatorStats);
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
        OperatorStats operatorStats = new OperatorStats();

        CpuTimer cpuTimer = new CpuTimer();
        long[] outputStats = execute(operatorStats);
        CpuDuration executionTime = cpuTimer.elapsedTime();

        SplitExecutionStats snapshot = operatorStats.snapshot();
        long inputRows = snapshot.getCompletedPositions().getTotalCount();
        long inputBytes = snapshot.getCompletedDataSize().getTotalCount();
        long outputRows = outputStats[0];
        long outputBytes = outputStats[1];

        double inputMegaBytes = new DataSize(inputBytes, BYTE).getValue(MEGABYTE);

        return ImmutableMap.<String, Long>builder()
                // legacy computed values
                .put("elapsed_millis", (long) executionTime.getWall().toMillis())
                .put("input_rows_per_second", (long) (inputRows / executionTime.getWall().convertTo(SECONDS)))
                .put("output_rows_per_second", (long) (outputRows / executionTime.getWall().convertTo(SECONDS)))
                .put("input_megabytes", (long) inputMegaBytes)
                .put("input_megabytes_per_second", (long) (inputMegaBytes / executionTime.getWall().convertTo(SECONDS)))

                .put("wall_nanos", (long) executionTime.getWall().convertTo(NANOSECONDS))
                .put("cpu_nanos", (long) executionTime.getCpu().convertTo(NANOSECONDS))
                .put("user_nanos", (long) executionTime.getUser().convertTo(NANOSECONDS))
                .put("input_rows", inputRows)
                .put("input_bytes", inputBytes)
                .put("output_rows", outputRows)
                .put("output_bytes", outputBytes)

                .build();
    }
}
