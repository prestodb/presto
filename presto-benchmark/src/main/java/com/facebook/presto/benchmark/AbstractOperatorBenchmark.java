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

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.DefaultQueryContext;
import com.facebook.presto.memory.MemoryPool;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PageSourceOperator;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskStats;
import com.facebook.presto.operator.project.InputPageProjection;
import com.facebook.presto.operator.project.InterpretedPageProjection;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.operator.project.PageProjection;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.split.SplitSource;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.optimizations.HashGenerationOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.airlift.stats.CpuTimer;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.SystemSessionProperties.getFilterAndProjectMinOutputPageRowCount;
import static com.facebook.presto.SystemSessionProperties.getFilterAndProjectMinOutputPageSize;
import static com.facebook.presto.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.stats.CpuTimer.CpuDuration;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Abstract template for benchmarks that want to test the performance of an Operator.
 */
public abstract class AbstractOperatorBenchmark
        extends AbstractBenchmark
{
    protected final LocalQueryRunner localQueryRunner;
    protected final Session session;

    protected AbstractOperatorBenchmark(
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        this(localQueryRunner.getDefaultSession(), localQueryRunner, benchmarkName, warmupIterations, measuredIterations);
    }

    protected AbstractOperatorBenchmark(
            Session session,
            LocalQueryRunner localQueryRunner,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations)
    {
        super(benchmarkName, warmupIterations, measuredIterations);
        this.localQueryRunner = requireNonNull(localQueryRunner, "localQueryRunner is null");

        TransactionId transactionId = localQueryRunner.getTransactionManager().beginTransaction(false);
        this.session = session.beginTransactionId(
                transactionId,
                localQueryRunner.getTransactionManager(),
                new AllowAllAccessControl());
    }

    @Override
    protected void tearDown()
    {
        localQueryRunner.getTransactionManager().asyncAbort(session.getRequiredTransactionId());
        super.tearDown();
    }

    protected final List<Type> getColumnTypes(String tableName, String... columnNames)
    {
        checkState(session.getCatalog().isPresent(), "catalog not set");
        checkState(session.getSchema().isPresent(), "schema not set");

        // look up the table
        Metadata metadata = localQueryRunner.getMetadata();
        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName);
        TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName)
                .orElseThrow(() -> new IllegalArgumentException(format("Table %s does not exist", qualifiedTableName)));

        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
        return Arrays.stream(columnNames)
                .map(allColumnHandles::get)
                .map(columnHandle -> metadata.getColumnMetadata(session, tableHandle, columnHandle).getType())
                .collect(toImmutableList());
    }

    protected final OperatorFactory createTableScanOperator(int operatorId, PlanNodeId planNodeId, String tableName, String... columnNames)
    {
        checkArgument(session.getCatalog().isPresent(), "catalog not set");
        checkArgument(session.getSchema().isPresent(), "schema not set");

        // look up the table
        Metadata metadata = localQueryRunner.getMetadata();
        QualifiedObjectName qualifiedTableName = new QualifiedObjectName(session.getCatalog().get(), session.getSchema().get(), tableName);
        TableHandle tableHandle = metadata.getTableHandle(session, qualifiedTableName).orElse(null);
        checkArgument(tableHandle != null, "Table %s does not exist", qualifiedTableName);

        // lookup the columns
        Map<String, ColumnHandle> allColumnHandles = metadata.getColumnHandles(session, tableHandle);
        ImmutableList.Builder<ColumnHandle> columnHandlesBuilder = ImmutableList.builder();
        for (String columnName : columnNames) {
            ColumnHandle columnHandle = allColumnHandles.get(columnName);
            checkArgument(columnHandle != null, "Table %s does not have a column %s", tableName, columnName);
            columnHandlesBuilder.add(columnHandle);
        }
        List<ColumnHandle> columnHandles = columnHandlesBuilder.build();

        // get the split for this table
        List<TableLayoutResult> layouts = metadata.getLayouts(session, tableHandle, Constraint.alwaysTrue(), Optional.empty());
        Split split = getLocalQuerySplit(session, layouts.get(0).getLayout().getHandle());

        return new OperatorFactory()
        {
            @Override
            public Operator createOperator(DriverContext driverContext)
            {
                OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, "BenchmarkSource");
                ConnectorPageSource pageSource = localQueryRunner.getPageSourceManager().createPageSource(session, split, columnHandles);
                return new PageSourceOperator(pageSource, operatorContext);
            }

            @Override
            public void noMoreOperators()
            {
            }

            @Override
            public OperatorFactory duplicate()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    private Split getLocalQuerySplit(Session session, TableLayoutHandle handle)
    {
        SplitSource splitSource = localQueryRunner.getSplitManager().getSplits(session, handle, UNGROUPED_SCHEDULING);
        List<Split> splits = new ArrayList<>();
        while (!splitSource.isFinished()) {
            splits.addAll(getNextBatch(splitSource));
        }
        checkArgument(splits.size() == 1, "Expected only one split for a local query, but got %s splits", splits.size());
        return splits.get(0);
    }

    private static List<Split> getNextBatch(SplitSource splitSource)
    {
        return getFutureValue(splitSource.getNextBatch(NOT_PARTITIONED, Lifespan.taskWide(), 1000)).getSplits();
    }

    protected final OperatorFactory createHashProjectOperator(int operatorId, PlanNodeId planNodeId, List<Type> types)
    {
        ImmutableMap.Builder<Symbol, Type> symbolTypes = ImmutableMap.builder();
        ImmutableMap.Builder<Symbol, Integer> symbolToInputMapping = ImmutableMap.builder();
        ImmutableList.Builder<PageProjection> projections = ImmutableList.builder();
        for (int channel = 0; channel < types.size(); channel++) {
            Symbol symbol = new Symbol("h" + channel);
            symbolTypes.put(symbol, types.get(channel));
            symbolToInputMapping.put(symbol, channel);
            projections.add(new InputPageProjection(channel, types.get(channel)));
        }

        Optional<Expression> hashExpression = HashGenerationOptimizer.getHashExpression(ImmutableList.copyOf(symbolTypes.build().keySet()));
        verify(hashExpression.isPresent());
        projections.add(new InterpretedPageProjection(
                hashExpression.get(),
                TypeProvider.copyOf(symbolTypes.build()),
                symbolToInputMapping.build(),
                localQueryRunner.getMetadata(),
                localQueryRunner.getSqlParser(),
                session));

        return new FilterAndProjectOperator.FilterAndProjectOperatorFactory(
                operatorId,
                planNodeId,
                () -> new PageProcessor(Optional.empty(), projections.build()),
                ImmutableList.copyOf(Iterables.concat(types, ImmutableList.of(BIGINT))),
                getFilterAndProjectMinOutputPageSize(session),
                getFilterAndProjectMinOutputPageRowCount(session));
    }

    protected abstract List<Driver> createDrivers(TaskContext taskContext);

    protected Map<String, Long> execute(TaskContext taskContext)
    {
        List<Driver> drivers = createDrivers(taskContext);

        long peakMemory = 0;
        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.process();
                    long lastPeakMemory = peakMemory;
                    peakMemory = (long) taskContext.getTaskStats().getUserMemoryReservation().getValue(BYTE);
                    if (peakMemory <= lastPeakMemory) {
                        peakMemory = lastPeakMemory;
                    }
                    processed = true;
                }
            }
            done = !processed;
        }
        return ImmutableMap.of("peak_memory", peakMemory);
    }

    @Override
    protected Map<String, Long> runOnce()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("optimizer.optimize-hash-generation", "true")
                .build();
        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1, GIGABYTE));
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));

        TaskContext taskContext = new DefaultQueryContext(
                new QueryId("test"),
                new DataSize(256, MEGABYTE),
                new DataSize(512, MEGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                new DataSize(256, MEGABYTE),
                spillSpaceTracker)
                .addTaskContext(new TaskStateMachine(new TaskId("query", 0, 0), localQueryRunner.getExecutor()),
                        session,
                        false,
                        false,
                        OptionalInt.empty());

        CpuTimer cpuTimer = new CpuTimer();
        Map<String, Long> executionStats = execute(taskContext);
        CpuDuration executionTime = cpuTimer.elapsedTime();

        TaskStats taskStats = taskContext.getTaskStats();
        long inputRows = taskStats.getRawInputPositions();
        long inputBytes = taskStats.getRawInputDataSize().toBytes();
        long outputRows = taskStats.getOutputPositions();
        long outputBytes = taskStats.getOutputDataSize().toBytes();

        double inputMegaBytes = new DataSize(inputBytes, BYTE).getValue(MEGABYTE);

        return ImmutableMap.<String, Long>builder()
                // legacy computed values
                .putAll(executionStats)
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
