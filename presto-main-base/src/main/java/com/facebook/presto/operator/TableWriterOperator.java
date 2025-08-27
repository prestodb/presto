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
package com.facebook.presto.operator;

import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.units.Duration;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget.CreateHandle;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget.InsertHandle;
import com.facebook.presto.execution.scheduler.ExecutionWriterTarget.RefreshMaterializedViewHandle;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.OperationTimer.OperationTiming;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PageSinkContext;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.util.AutoCloseableCloser;
import com.facebook.presto.util.Mergeable;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.airlift.units.Duration.succinctNanos;
import static com.facebook.presto.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static com.facebook.presto.common.RuntimeMetricName.WRITTEN_FILES_COUNT;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.TableWriterUtils.STATS_START_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.createStatisticsPage;
import static com.facebook.presto.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.allAsList;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class TableWriterOperator
        implements Operator
{
    public static final String OPERATOR_TYPE = "TableWriterOperator";

    public static class TableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final ExecutionWriterTarget target;
        private final List<Integer> columnChannels;
        private final List<String> notNullChannelColumnNames;
        private final Session session;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final List<Type> types;
        private final PageSinkCommitStrategy pageSinkCommitStrategy;
        private boolean closed;
        private final JsonCodec<TableCommitContext> tableCommitContextCodec;

        public TableWriterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                ExecutionWriterTarget writerTarget,
                List<Integer> columnChannels,
                List<String> notNullChannelColumnNames,
                Session session,
                OperatorFactory statisticsAggregationOperatorFactory,
                List<Type> types,
                JsonCodec<TableCommitContext> tableCommitContextCodec,
                PageSinkCommitStrategy pageSinkCommitStrategy)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
            this.notNullChannelColumnNames = requireNonNull(notNullChannelColumnNames, "notNullChannelColumnNames is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            checkArgument(
                    writerTarget instanceof CreateHandle || writerTarget instanceof InsertHandle || writerTarget instanceof RefreshMaterializedViewHandle,
                    "writerTarget must be CreateHandle or InsertHandle or RefreshMaterializedViewHandle");
            this.target = requireNonNull(writerTarget, "writerTarget is null");
            this.session = session;
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
            this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
            this.pageSinkCommitStrategy = requireNonNull(pageSinkCommitStrategy, "pageSinkCommitStrategy is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableWriterOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableWriterOperator(
                    context,
                    createPageSink(context),
                    columnChannels,
                    notNullChannelColumnNames,
                    statisticsAggregationOperator,
                    types,
                    statisticsCpuTimerEnabled,
                    tableCommitContextCodec,
                    pageSinkCommitStrategy);
        }

        private ConnectorPageSink createPageSink(OperatorContext operatorContext)
        {
            PageSinkContext.Builder pageSinkContextBuilder = PageSinkContext.builder()
                    .setCommitRequired(pageSinkCommitStrategy.isCommitRequired());

            RuntimeStats runtimeStats = operatorContext.getRuntimeStats();

            if (target instanceof CreateHandle) {
                return pageSinkManager.createPageSink(session, ((CreateHandle) target).getHandle(), pageSinkContextBuilder.build(), runtimeStats);
            }
            if (target instanceof InsertHandle) {
                return pageSinkManager.createPageSink(session, ((InsertHandle) target).getHandle(), pageSinkContextBuilder.build(), runtimeStats);
            }
            if (target instanceof RefreshMaterializedViewHandle) {
                return pageSinkManager.createPageSink(session, ((RefreshMaterializedViewHandle) target).getHandle(), pageSinkContextBuilder.build(), runtimeStats);
            }
            throw new UnsupportedOperationException("Unhandled target type: " + target.getClass().getName());
        }

        private static ConnectorId getConnectorId(ExecutionWriterTarget handle)
        {
            if (handle instanceof CreateHandle) {
                return ((CreateHandle) handle).getHandle().getConnectorId();
            }

            if (handle instanceof InsertHandle) {
                return ((InsertHandle) handle).getHandle().getConnectorId();
            }

            if (handle instanceof RefreshMaterializedViewHandle) {
                return ((RefreshMaterializedViewHandle) handle).getHandle().getConnectorId();
            }

            throw new UnsupportedOperationException("Unhandled target type: " + handle.getClass().getName());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableWriterOperatorFactory(
                    operatorId,
                    planNodeId,
                    pageSinkManager,
                    target,
                    columnChannels,
                    notNullChannelColumnNames,
                    session,
                    statisticsAggregationOperatorFactory,
                    types,
                    tableCommitContextCodec,
                    pageSinkCommitStrategy);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final LocalMemoryContext pageSinkMemoryContext;
    private final ConnectorPageSink pageSink;
    private final List<Integer> columnChannels;
    private final List<String> notNullChannelColumnNames;
    private final AtomicLong pageSinkPeakMemoryUsage = new AtomicLong();
    private final Operator statisticAggregationOperator;
    private final List<Type> types;

    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private CompletableFuture<Collection<Slice>> finishFuture;
    private State state = State.RUNNING;
    private long rowCount;
    private boolean committed;
    private boolean closed;
    private long writtenBytes;

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final JsonCodec<TableCommitContext> tableCommitContextCodec;
    private final PageSinkCommitStrategy pageSinkCommitStrategy;

    private final Supplier<TableWriterInfo> tableWriterInfoSupplier;

    public TableWriterOperator(
            OperatorContext operatorContext,
            ConnectorPageSink pageSink,
            List<Integer> columnChannels,
            List<String> notNullChannelColumnNames,
            Operator statisticAggregationOperator,
            List<Type> types,
            boolean statisticsCpuTimerEnabled,
            JsonCodec<TableCommitContext> tableCommitContextCodec,
            PageSinkCommitStrategy pageSinkCommitStrategy)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageSinkMemoryContext = operatorContext.localSystemMemoryContext();
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
        this.notNullChannelColumnNames = requireNonNull(notNullChannelColumnNames, "notNullChannelColumnNames is null");
        checkArgument(columnChannels.size() == notNullChannelColumnNames.size(), "columnChannels and notNullColumnNames have different sizes");
        this.statisticAggregationOperator = requireNonNull(statisticAggregationOperator, "statisticAggregationOperator is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        this.pageSinkCommitStrategy = requireNonNull(pageSinkCommitStrategy, "pageSinkCommitStrategy is null");
        this.tableWriterInfoSupplier = createTableWriterInfoSupplier(pageSinkPeakMemoryUsage, statisticsTiming, pageSink);
        this.operatorContext.setInfoSupplier(tableWriterInfoSupplier);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        ListenableFuture<?> currentlyBlocked = blocked;

        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticAggregationOperator.finish();
        timer.end(statisticsTiming);

        ListenableFuture<?> blockedOnAggregation = statisticAggregationOperator.isBlocked();
        ListenableFuture<?> blockedOnFinish = NOT_BLOCKED;
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = pageSink.finish();
            blockedOnFinish = toListenableFuture(finishFuture);
            updateWrittenBytes();
        }
        this.blocked = allAsList(currentlyBlocked, blockedOnAggregation, blockedOnFinish);
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED && blocked.isDone();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING || !blocked.isDone()) {
            return false;
        }
        return statisticAggregationOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator does not need input");

        Block[] blocks = new Block[columnChannels.size()];
        for (int outputChannel = 0; outputChannel < columnChannels.size(); outputChannel++) {
            Block block = page.getBlock(columnChannels.get(outputChannel));
            String columnName = notNullChannelColumnNames.get(outputChannel);
            if (columnName != null) {
                verifyBlockHasNoNulls(block, columnName);
            }
            blocks[outputChannel] = block;
        }

        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticAggregationOperator.addInput(page);
        timer.end(statisticsTiming);

        ListenableFuture<?> blockedOnAggregation = statisticAggregationOperator.isBlocked();
        CompletableFuture<?> future = pageSink.appendPage(new Page(blocks));
        updateMemoryUsage();
        ListenableFuture<?> blockedOnWrite = toListenableFuture(future);
        blocked = allAsList(blockedOnAggregation, blockedOnWrite);
        rowCount += page.getPositionCount();
        updateWrittenBytes();
    }

    private void verifyBlockHasNoNulls(Block block, String columnName)
    {
        if (!block.mayHaveNull()) {
            return;
        }
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                throw new PrestoException(CONSTRAINT_VIOLATION, "NULL value not allowed for NOT NULL column: " + columnName);
            }
        }
    }

    @Override
    public Page getOutput()
    {
        if (!blocked.isDone()) {
            return null;
        }

        if (!statisticAggregationOperator.isFinished()) {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            Page aggregationOutput = statisticAggregationOperator.getOutput();
            timer.end(statisticsTiming);

            if (aggregationOutput == null) {
                return null;
            }
            return createStatisticsPage(types, aggregationOutput, createTableCommitContext(false));
        }

        if (state != State.FINISHING) {
            return null;
        }

        Page fragmentsPage = createFragmentsPage();
        int positionCount = fragmentsPage.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel < STATS_START_CHANNEL) {
                outputBlocks[channel] = fragmentsPage.getBlock(channel);
            }
            else {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
        }

        updateWrittenFilesCount();
        state = State.FINISHED;
        return new Page(positionCount, outputBlocks);
    }

    // Fragments page layout:
    //
    // row     fragments     context
    //  X         null          X
    // null        X            X
    // null        X            X
    // null        X            X
    // ...
    private Page createFragmentsPage()
    {
        Collection<Slice> fragments = getFutureValue(finishFuture);
        int positionCount = fragments.size() + 1;
        committed = true;
        updateWrittenBytes();

        // Output page will only be constructed once, and the table commit context channel will be constructed using RunLengthEncodedBlock.
        // Thus individual BlockBuilder is used for each channel, instead of using PageBuilder.
        BlockBuilder rowsBuilder = BIGINT.createBlockBuilder(null, positionCount);
        BlockBuilder fragmentBuilder = VARBINARY.createBlockBuilder(null, positionCount);

        // write row count
        BIGINT.writeLong(rowsBuilder, rowCount);
        fragmentBuilder.appendNull();

        // write fragments
        for (Slice fragment : fragments) {
            rowsBuilder.appendNull();
            VARBINARY.writeSlice(fragmentBuilder, fragment);
        }

        return new Page(positionCount, rowsBuilder.build(), fragmentBuilder.build(), RunLengthEncodedBlock.create(VARBINARY, createTableCommitContext(true), positionCount));
    }

    private Slice createTableCommitContext(boolean lastPage)
    {
        TaskId taskId = operatorContext.getDriverContext().getPipelineContext().getTaskId();
        return wrappedBuffer(tableCommitContextCodec.toJsonBytes(
                new TableCommitContext(
                        operatorContext.getDriverContext().getLifespan(),
                        taskId,
                        pageSinkCommitStrategy,
                        lastPage)));
    }

    @Override
    public void close()
            throws Exception
    {
        AutoCloseableCloser closer = AutoCloseableCloser.create();
        if (!closed) {
            closed = true;
            if (!committed) {
                closer.register(pageSink::abort);
            }
        }
        closer.register(statisticAggregationOperator);
        closer.register(() -> pageSinkMemoryContext.setBytes(0));
        closer.close();
    }

    private void updateWrittenBytes()
    {
        long current = pageSink.getCompletedBytes();
        operatorContext.recordPhysicalWrittenData(current - writtenBytes);
        writtenBytes = current;
    }

    private void updateWrittenFilesCount()
    {
        operatorContext.getRuntimeStats().addMetricValue(WRITTEN_FILES_COUNT, NONE, pageSink.getWrittenFilesCount());
    }

    private void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = pageSink.getSystemMemoryUsage();
        pageSinkMemoryContext.setBytes(pageSinkMemoryUsage);
        pageSinkPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    @VisibleForTesting
    Operator getStatisticAggregationOperator()
    {
        return statisticAggregationOperator;
    }

    @VisibleForTesting
    TableWriterInfo getInfo()
    {
        return tableWriterInfoSupplier.get();
    }

    private static Supplier<TableWriterInfo> createTableWriterInfoSupplier(AtomicLong pageSinkPeakMemoryUsage, OperationTiming statisticsTiming, ConnectorPageSink pageSink)
    {
        requireNonNull(pageSinkPeakMemoryUsage, "pageSinkPeakMemoryUsage is null");
        requireNonNull(statisticsTiming, "statisticsTiming is null");
        requireNonNull(pageSink, "pageSink is null");
        return () -> new TableWriterInfo(
                pageSinkPeakMemoryUsage.get(),
                succinctNanos(statisticsTiming.getWallNanos()),
                succinctNanos(statisticsTiming.getCpuNanos()),
                succinctNanos(pageSink.getValidationCpuNanos()));
    }

    @ThriftStruct
    public static class TableWriterInfo
            implements Mergeable<TableWriterInfo>, OperatorInfo
    {
        private final long pageSinkPeakMemoryUsage;
        private final Duration statisticsWallTime;
        private final Duration statisticsCpuTime;
        private final Duration validationCpuTime;

        @JsonCreator
        @ThriftConstructor
        public TableWriterInfo(
                @JsonProperty("pageSinkPeakMemoryUsage") long pageSinkPeakMemoryUsage,
                @JsonProperty("statisticsWallTime") Duration statisticsWallTime,
                @JsonProperty("statisticsCpuTime") Duration statisticsCpuTime,
                @JsonProperty("validationCpuTime") Duration validationCpuTime)
        {
            this.pageSinkPeakMemoryUsage = pageSinkPeakMemoryUsage;
            this.statisticsWallTime = requireNonNull(statisticsWallTime, "statisticsWallTime is null");
            this.statisticsCpuTime = requireNonNull(statisticsCpuTime, "statisticsCpuTime is null");
            this.validationCpuTime = requireNonNull(validationCpuTime, "validationCpuTime is null");
        }

        @JsonProperty
        @ThriftField(1)
        public long getPageSinkPeakMemoryUsage()
        {
            return pageSinkPeakMemoryUsage;
        }

        @JsonProperty
        @ThriftField(2)
        public Duration getStatisticsWallTime()
        {
            return statisticsWallTime;
        }

        @JsonProperty
        @ThriftField(3)
        public Duration getStatisticsCpuTime()
        {
            return statisticsCpuTime;
        }

        @JsonProperty
        @ThriftField(4)
        public Duration getValidationCpuTime()
        {
            return validationCpuTime;
        }

        @Override
        public TableWriterInfo mergeWith(TableWriterInfo other)
        {
            return new TableWriterInfo(
                    Math.max(pageSinkPeakMemoryUsage, other.pageSinkPeakMemoryUsage),
                    succinctNanos(statisticsWallTime.roundTo(NANOSECONDS) + other.statisticsWallTime.roundTo(NANOSECONDS)),
                    succinctNanos(statisticsCpuTime.roundTo(NANOSECONDS) + other.statisticsCpuTime.roundTo(NANOSECONDS)),
                    succinctNanos(validationCpuTime.roundTo(NANOSECONDS) + other.validationCpuTime.roundTo(NANOSECONDS)));
        }

        @Override
        public boolean isFinal()
        {
            return true;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pageSinkPeakMemoryUsage", pageSinkPeakMemoryUsage)
                    .add("statisticsWallTime", statisticsWallTime)
                    .add("statisticsCpuTime", statisticsCpuTime)
                    .add("validationCpuTime", validationCpuTime)
                    .toString();
        }
    }
}
