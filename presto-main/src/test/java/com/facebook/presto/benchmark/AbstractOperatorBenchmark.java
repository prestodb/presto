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
package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.analyzer.Session;
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.tpch.TpchMetadata.TPCH_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
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
    public static final TpchBlocksProvider DEFAULT_TPCH_BLOCKS_PROVIDER = new DataFileTpchBlocksProvider(new CachingTpchDataFileLoader(new GeneratingTpchDataFileLoader()));

    private final ExecutorService executor;
    private final TpchBlocksProvider tpchBlocksProvider;

    protected AbstractOperatorBenchmark(
            ExecutorService executor,
            TpchBlocksProvider tpchBlocksProvider,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
        this.executor = checkNotNull(executor, "executor is null");
        this.tpchBlocksProvider = checkNotNull(tpchBlocksProvider, "tpchBlocksProvider is null");
    }

    protected TpchBlocksProvider getTpchBlocksProvider()
    {
        return tpchBlocksProvider;
    }

    protected BlockIterable getBlockIterable(String tableName, String columnName, BlocksFileEncoding columnEncoding)
    {
        ConnectorMetadata metadata = new TpchMetadata();
        TableHandle tableHandle = metadata.getTableHandle(new SchemaTableName(TPCH_SCHEMA_NAME, tableName));
        ColumnHandle columnHandle = metadata.getColumnHandle(tableHandle, columnName);
        checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
        return getTpchBlocksProvider().getBlocks((TpchTableHandle) tableHandle, (TpchColumnHandle) columnHandle, 0, 1, columnEncoding);
    }

    protected abstract List<Driver> createDrivers(TaskContext taskContext);

    protected void execute(TaskContext taskContext)
    {
        List<Driver> drivers = createDrivers(taskContext);

        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.process();
                    processed = true;
                }
            }
            done = !processed;
        }
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        TaskContext taskContext = new TaskContext(
                new TaskStateMachine(new TaskId("query", "stage", "task"), executor),
                executor,
                session,
                new DataSize(256, MEGABYTE),
                new DataSize(1, MEGABYTE),
                false);

        CpuTimer cpuTimer = new CpuTimer();
        execute(taskContext);
        CpuDuration executionTime = cpuTimer.elapsedTime();

        TaskStats taskStats = taskContext.getTaskStats();
        long inputRows = taskStats.getRawInputPositions();
        long inputBytes = taskStats.getRawInputDataSize().toBytes();
        long outputRows = taskStats.getOutputPositions();
        long outputBytes = taskStats.getOutputDataSize().toBytes();

        double inputMegaBytes = new DataSize(inputBytes, BYTE).getValue(MEGABYTE);

        return ImmutableMap.<String, Long>builder()
                // legacy computed values
                .put("elapsed_millis", executionTime.getWall().toMillis())
                .put("input_rows_per_second", (long) (inputRows / executionTime.getWall().getValue(SECONDS)))
                .put("output_rows_per_second", (long) (outputRows / executionTime.getWall().getValue(SECONDS)))
                .put("input_megabytes", (long) inputMegaBytes)
                .put("input_megabytes_per_second", (long) (inputMegaBytes / executionTime.getWall().getValue(SECONDS)))

                .put("wall_nanos", executionTime.getWall().roundTo(NANOSECONDS))
                .put("cpu_nanos", executionTime.getCpu().roundTo(NANOSECONDS))
                .put("user_nanos", executionTime.getUser().roundTo(NANOSECONDS))
                .put("input_rows", inputRows)
                .put("input_bytes", inputBytes)
                .put("output_rows", outputRows)
                .put("output_bytes", outputBytes)

                .build();
    }
}
