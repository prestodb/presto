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
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.OperationTimer.OperationTiming;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.PageSinkCommitStrategy.LIFESPAN_COMMIT;
import static com.facebook.presto.operator.TableWriterUtils.FRAGMENT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.ROW_COUNT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.extractStatisticsRows;
import static com.facebook.presto.operator.TableWriterUtils.getTableCommitContext;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagateIfPossible;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.Futures.whenAllSucceed;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.Duration.succinctNanos;
import static java.lang.Thread.currentThread;
import static java.util.Objects.requireNonNull;

public class TableFinishOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.of(BIGINT);

    public static class TableFinishOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final TableFinisher tableFinisher;
        private final PageSinkCommitter pageSinkCommitter;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final StatisticAggregationsDescriptor<Integer> descriptor;
        private final Session session;
        private final JsonCodec<TableCommitContext> tableCommitContextCodec;
        private final boolean memoryTrackingEnabled;

        private boolean closed;

        public TableFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFinisher tableFinisher,
                PageSinkCommitter pageSinkCommitter,
                OperatorFactory statisticsAggregationOperatorFactory,
                StatisticAggregationsDescriptor<Integer> descriptor,
                Session session,
                JsonCodec<TableCommitContext> tableCommitContextCodec,
                boolean memoryTrackingEnabled)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.tableFinisher = requireNonNull(tableFinisher, "tableFinisher is null");
            this.pageSinkCommitter = requireNonNull(pageSinkCommitter, "pageSinkCommitter is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.session = requireNonNull(session, "session is null");
            this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
            this.memoryTrackingEnabled = memoryTrackingEnabled;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableFinishOperator(
                    context,
                    tableFinisher,
                    pageSinkCommitter,
                    statisticsAggregationOperator,
                    descriptor,
                    statisticsCpuTimerEnabled,
                    memoryTrackingEnabled,
                    tableCommitContextCodec);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFinishOperatorFactory(
                    operatorId,
                    planNodeId,
                    tableFinisher,
                    pageSinkCommitter,
                    statisticsAggregationOperatorFactory,
                    descriptor,
                    session,
                    tableCommitContextCodec,
                    memoryTrackingEnabled);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final TableFinisher tableFinisher;
    private final Operator statisticsAggregationOperator;
    private final StatisticAggregationsDescriptor<Integer> descriptor;

    private State state = State.RUNNING;
    private final AtomicReference<Optional<ConnectorOutputMetadata>> outputMetadata = new AtomicReference<>(Optional.empty());
    private final ImmutableList.Builder<ComputedStatistics> computedStatisticsBuilder = ImmutableList.builder();

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;
    private final boolean memoryTrackingEnabled;

    private final JsonCodec<TableCommitContext> tableCommitContextCodec;
    private final LifespanAndStageStateTracker lifespanAndStageStateTracker;

    private final LocalMemoryContext systemMemoryContext;
    private final AtomicLong operatorRetainedMemoryBytes = new AtomicLong();

    private final Supplier<TableFinishInfo> tableFinishInfoSupplier;

    public TableFinishOperator(
            OperatorContext operatorContext,
            TableFinisher tableFinisher,
            PageSinkCommitter pageSinkCommitter,
            Operator statisticsAggregationOperator,
            StatisticAggregationsDescriptor<Integer> descriptor,
            boolean statisticsCpuTimerEnabled,
            boolean memoryTrackingEnabled,
            JsonCodec<TableCommitContext> tableCommitContextCodec)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
        this.statisticsAggregationOperator = requireNonNull(statisticsAggregationOperator, "statisticsAggregationOperator is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.memoryTrackingEnabled = memoryTrackingEnabled;
        this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        this.lifespanAndStageStateTracker = new LifespanAndStageStateTracker(pageSinkCommitter, operatorRetainedMemoryBytes);
        this.systemMemoryContext = operatorContext.localSystemMemoryContext();
        this.tableFinishInfoSupplier = createTableFinishInfoSupplier(outputMetadata, statisticsTiming);
        operatorContext.setInfoSupplier(tableFinishInfoSupplier);
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
        statisticsAggregationOperator.finish();
        timer.end(statisticsTiming);

        if (state == State.RUNNING) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        if (state == State.FINISHED) {
            verify(statisticsAggregationOperator.isFinished());
            return true;
        }
        return false;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return statisticsAggregationOperator.isBlocked();
    }

    @Override
    public boolean needsInput()
    {
        if (state != State.RUNNING) {
            return false;
        }
        return statisticsAggregationOperator.needsInput();
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        TableCommitContext tableCommitContext = getTableCommitContext(page, tableCommitContextCodec);
        lifespanAndStageStateTracker.update(page, tableCommitContext);
        lifespanAndStageStateTracker.getStatisticsPagesToProcess(page, tableCommitContext).forEach(statisticsPage -> {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            statisticsAggregationOperator.addInput(statisticsPage);
            timer.end(statisticsTiming);
        });
        if (memoryTrackingEnabled) {
            systemMemoryContext.setBytes(operatorRetainedMemoryBytes.get());
        }
    }

    @Override
    public Page getOutput()
    {
        if (!isBlocked().isDone()) {
            return null;
        }

        if (!statisticsAggregationOperator.isFinished()) {
            verify(statisticsAggregationOperator.isBlocked().isDone(), "aggregation operator should not be blocked");

            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            Page page = statisticsAggregationOperator.getOutput();
            timer.end(statisticsTiming);

            if (page == null) {
                return null;
            }
            for (int position = 0; position < page.getPositionCount(); position++) {
                computedStatisticsBuilder.add(getComputedStatistics(page, position));
            }
            return null;
        }

        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        lifespanAndStageStateTracker.commit();
        outputMetadata.set(tableFinisher.finishTable(lifespanAndStageStateTracker.getFinalFragments(), computedStatisticsBuilder.build()));

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(1, TYPES);
        page.declarePosition();
        BIGINT.writeLong(page.getBlockBuilder(0), lifespanAndStageStateTracker.getFinalRowCount());
        return page.build();
    }

    private ComputedStatistics getComputedStatistics(Page page, int position)
    {
        ImmutableList.Builder<String> groupingColumns = ImmutableList.builder();
        ImmutableList.Builder<Block> groupingValues = ImmutableList.builder();
        descriptor.getGrouping().forEach((column, channel) -> {
            groupingColumns.add(column);
            groupingValues.add(page.getBlock(channel).getSingleValueBlock(position));
        });

        ComputedStatistics.Builder statistics = ComputedStatistics.builder(groupingColumns.build(), groupingValues.build());

        descriptor.getTableStatistics().forEach((type, channel) ->
                statistics.addTableStatistic(type, page.getBlock(channel).getSingleValueBlock(position)));

        descriptor.getColumnStatistics().forEach((metadata, channel) -> statistics.addColumnStatistic(metadata, page.getBlock(channel).getSingleValueBlock(position)));

        return statistics.build();
    }

    @VisibleForTesting
    TableFinishInfo getInfo()
    {
        return tableFinishInfoSupplier.get();
    }

    private static Supplier<TableFinishInfo> createTableFinishInfoSupplier(AtomicReference<Optional<ConnectorOutputMetadata>> outputMetadata, OperationTiming statisticsTiming)
    {
        requireNonNull(outputMetadata, "outputMetadata is null");
        requireNonNull(statisticsTiming, "statisticsTiming is null");
        return () -> new TableFinishInfo(
                outputMetadata.get(),
                succinctNanos(statisticsTiming.getWallNanos()),
                succinctNanos(statisticsTiming.getCpuNanos()));
    }

    @Override
    public void close()
            throws Exception
    {
        statisticsAggregationOperator.close();
        systemMemoryContext.setBytes(0);
    }

    public interface TableFinisher
    {
        Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);
    }

    public interface PageSinkCommitter
    {
        ListenableFuture<Void> commitAsync(Collection<Slice> fragments);
    }

    // A lifespan in a stage defines the unit for commit and recovery in recoverable grouped execution
    private static class LifespanAndStageStateTracker
    {
        private final Map<LifespanAndStage, LifespanAndStageState> noCommitUnrecoverableLifespanAndStageStates = new HashMap<>();
        private final Map<LifespanAndStage, LifespanAndStageState> taskCommitUnrecoverableLifespanAndStageStates = new HashMap<>();

        // For recoverable execution, it is possible to receive pages of the same lifespan-stage from different tasks. We track all of them and commit the one
        // which finishes sending pages first.
        private final Map<LifespanAndStage, Map<TaskId, LifespanAndStageState>> uncommittedRecoverableLifespanAndStageStates = new HashMap<>();
        private final Map<LifespanAndStage, LifespanAndStageState> committedRecoverableLifespanAndStages = new HashMap<>();

        private final PageSinkCommitter pageSinkCommitter;
        private final List<ListenableFuture<Void>> commitFutures = new ArrayList<>();
        private final AtomicLong operatorRetainedMemoryBytes;

        LifespanAndStageStateTracker(
                PageSinkCommitter pageSinkCommitter,
                AtomicLong operatorRetainedMemoryBytes)
        {
            this.pageSinkCommitter = requireNonNull(pageSinkCommitter, "pageSinkCommitter is null");
            this.operatorRetainedMemoryBytes = requireNonNull(operatorRetainedMemoryBytes, "operatorRetainedMemoryBytes is null");
        }

        public void commit()
        {
            for (LifespanAndStageState lifespanAndStageState : taskCommitUnrecoverableLifespanAndStageStates.values()) {
                commitFutures.add(pageSinkCommitter.commitAsync(lifespanAndStageState.getFragments()));
            }

            ListenableFuture<Void> future = whenAllSucceed(commitFutures).call(() -> null, directExecutor());
            try {
                future.get();
            }
            catch (InterruptedException e) {
                future.cancel(true);
                currentThread().interrupt();
                throw new RuntimeException(e);
            }
            catch (ExecutionException e) {
                future.cancel(true);
                propagateIfPossible(e.getCause(), PrestoException.class);
                throw new RuntimeException(e.getCause());
            }
        }

        public void update(Page page, TableCommitContext tableCommitContext)
        {
            LifespanAndStage lifespanAndStage = LifespanAndStage.fromTableCommitContext(tableCommitContext);
            PageSinkCommitStrategy commitStrategy = tableCommitContext.getPageSinkCommitStrategy();
            switch (commitStrategy) {
                case NO_COMMIT: {
                    // Case 1: lifespan commit is not required, this can be one of the following cases:
                    //  - The source fragment is ungrouped execution (lifespan is TASK_WIDE).
                    //  - The source fragment is grouped execution but not recoverable.
                    noCommitUnrecoverableLifespanAndStageStates.computeIfAbsent(
                            lifespanAndStage, ignored -> new LifespanAndStageState(
                                    tableCommitContext.getTaskId(), operatorRetainedMemoryBytes, false)).update(page);
                    return;
                }
                case TASK_COMMIT: {
                    // Case 2: Commit is required, but partial recovery is not supported
                    taskCommitUnrecoverableLifespanAndStageStates.computeIfAbsent(
                            lifespanAndStage, ignored -> new LifespanAndStageState(
                                    tableCommitContext.getTaskId(), operatorRetainedMemoryBytes, false)).update(page);
                    return;
                }
                case LIFESPAN_COMMIT: {
                    // Case 2: Lifespan commit is required
                    checkState(lifespanAndStage.lifespan != Lifespan.taskWide(), "Recoverable lifespan cannot be TASK_WIDE");

                    // Case 2a: Current (stage, lifespan) combination is already committed
                    if (committedRecoverableLifespanAndStages.containsKey(lifespanAndStage)) {
                        checkState(
                                !committedRecoverableLifespanAndStages.get(lifespanAndStage).getTaskId().equals(tableCommitContext.getTaskId()),
                                "Received page from same task of committed lifespan and stage combination");
                        return;
                    }

                    // Case 2b: Current (stage, lifespan) combination is not yet committed
                    Map<TaskId, LifespanAndStageState> lifespanStageStatesPerTask = uncommittedRecoverableLifespanAndStageStates.computeIfAbsent(lifespanAndStage, ignored -> new HashMap<>());
                    lifespanStageStatesPerTask.computeIfAbsent(
                            tableCommitContext.getTaskId(), ignored -> new LifespanAndStageState(
                                    tableCommitContext.getTaskId(), operatorRetainedMemoryBytes, true)).update(page);

                    if (tableCommitContext.isLastPage()) {
                        checkState(!committedRecoverableLifespanAndStages.containsKey(lifespanAndStage), "LifespanAndStage already finished");
                        LifespanAndStageState lifespanAndStageState = lifespanStageStatesPerTask.get(tableCommitContext.getTaskId());
                        committedRecoverableLifespanAndStages.put(lifespanAndStage, lifespanAndStageState);
                        uncommittedRecoverableLifespanAndStageStates.remove(lifespanAndStage);
                        commitFutures.add(pageSinkCommitter.commitAsync(lifespanAndStageState.getFragments()));
                    }
                    return;
                }
                default:
                    throw new IllegalArgumentException("unexpected commit strategy: " + commitStrategy);
            }
        }

        List<Page> getStatisticsPagesToProcess(Page page, TableCommitContext tableCommitContext)
        {
            LifespanAndStage lifespanAndStage = LifespanAndStage.fromTableCommitContext(tableCommitContext);
            if (tableCommitContext.getPageSinkCommitStrategy() != LIFESPAN_COMMIT) {
                return extractStatisticsRows(page).map(ImmutableList::of).orElse(ImmutableList.of());
            }
            if (!committedRecoverableLifespanAndStages.containsKey(lifespanAndStage)) {
                return ImmutableList.of();
            }
            checkState(!uncommittedRecoverableLifespanAndStageStates.containsKey(lifespanAndStage), "lifespanAndStage %s is already committed", lifespanAndStage);
            LifespanAndStageState lifespanAndStageState = committedRecoverableLifespanAndStages.get(lifespanAndStage);
            List<Page> pages = lifespanAndStageState.getStatisticsPages();
            // statistics data in this lifespanAndStageState will not be accessed any more
            lifespanAndStageState.resetStatisticsPages();
            return pages;
        }

        public long getFinalRowCount()
        {
            checkState(uncommittedRecoverableLifespanAndStageStates.isEmpty(), "All recoverable LifespanAndStage should be committed when fetching final row count");
            return Streams.concat(
                    noCommitUnrecoverableLifespanAndStageStates.values().stream(),
                    taskCommitUnrecoverableLifespanAndStageStates.values().stream(),
                    committedRecoverableLifespanAndStages.values().stream())
                    .mapToLong(LifespanAndStageState::getRowCount)
                    .sum();
        }

        public List<Slice> getFinalFragments()
        {
            checkState(uncommittedRecoverableLifespanAndStageStates.isEmpty(), "All recoverable LifespanAndStage should be committed when fetching final fragments");
            return Streams.concat(
                    noCommitUnrecoverableLifespanAndStageStates.values().stream(),
                    taskCommitUnrecoverableLifespanAndStageStates.values().stream(),
                    committedRecoverableLifespanAndStages.values().stream())
                    .map(LifespanAndStageState::getFragments)
                    .flatMap(List::stream)
                    .collect(toImmutableList());
        }

        private static class LifespanAndStage
        {
            private final Lifespan lifespan;
            private final int stageId;

            private LifespanAndStage(Lifespan lifespan, int stageId)
            {
                this.lifespan = requireNonNull(lifespan, "lifespan is null");
                this.stageId = stageId;
            }

            public static LifespanAndStage fromTableCommitContext(TableCommitContext operatorExecutionContext)
            {
                return new LifespanAndStage(operatorExecutionContext.getLifespan(), operatorExecutionContext.getTaskId().getStageExecutionId().getStageId().getId());
            }

            public Lifespan getLifespan()
            {
                return lifespan;
            }

            public int getStageId()
            {
                return stageId;
            }

            @Override
            public boolean equals(Object o)
            {
                if (this == o) {
                    return true;
                }
                if (!(o instanceof LifespanAndStage)) {
                    return false;
                }
                LifespanAndStage that = (LifespanAndStage) o;
                return stageId == that.stageId &&
                        Objects.equals(lifespan, that.lifespan);
            }

            @Override
            public int hashCode()
            {
                return Objects.hash(lifespan, stageId);
            }

            @Override
            public String toString()
            {
                return toStringHelper(this)
                        .add("lifespan", lifespan)
                        .add("stageId", stageId)
                        .toString();
            }
        }

        private static class LifespanAndStageState
        {
            private final ImmutableList.Builder<Slice> fragmentBuilder = ImmutableList.builder();
            private final TaskId taskId;
            private final AtomicLong operatorRetainedMemoryBytes;

            private long retainedMemoryBytesForStatisticsPages;
            private long rowCount;
            private Optional<ImmutableList.Builder<Page>> statisticsPages;

            public LifespanAndStageState(
                    TaskId taskId,
                    AtomicLong operatorRetainedMemoryBytes,
                    boolean trackStatisticsPages)
            {
                this.taskId = requireNonNull(taskId, "taskId is null");
                this.operatorRetainedMemoryBytes = requireNonNull(operatorRetainedMemoryBytes, "operatorRetainedMemoryBytes is null");
                this.statisticsPages = trackStatisticsPages ? Optional.of(ImmutableList.builder()) : Optional.empty();
            }

            public void update(Page page)
            {
                long memoryBytesDelta = 0;
                Block rowCountBlock = page.getBlock(ROW_COUNT_CHANNEL);
                Block fragmentBlock = page.getBlock(FRAGMENT_CHANNEL);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!rowCountBlock.isNull(position)) {
                        rowCount += BIGINT.getLong(rowCountBlock, position);
                    }
                    if (!fragmentBlock.isNull(position)) {
                        Slice fragment = VARBINARY.getSlice(fragmentBlock, position);
                        fragmentBuilder.add(fragment);
                        memoryBytesDelta += fragment.getRetainedSize();
                    }
                }
                if (statisticsPages.isPresent()) {
                    Optional<Page> statisticsPage = extractStatisticsRows(page);
                    if (statisticsPage.isPresent()) {
                        statisticsPages.get().add(statisticsPage.get());
                        long retainedSizeForStatisticsPage = statisticsPage.get().getRetainedSizeInBytes();
                        retainedMemoryBytesForStatisticsPages += retainedSizeForStatisticsPage;
                        memoryBytesDelta += retainedSizeForStatisticsPage;
                    }
                }
                operatorRetainedMemoryBytes.addAndGet(memoryBytesDelta);
            }

            public long getRowCount()
            {
                return rowCount;
            }

            public List<Slice> getFragments()
            {
                return fragmentBuilder.build();
            }

            public List<Page> getStatisticsPages()
            {
                checkState(statisticsPages.isPresent(), "statisticsPages is present for recoverable grouped execution only");
                return statisticsPages.get().build();
            }

            public TaskId getTaskId()
            {
                return taskId;
            }

            public void resetStatisticsPages()
            {
                statisticsPages = Optional.empty();
                operatorRetainedMemoryBytes.addAndGet(-retainedMemoryBytesForStatisticsPages);
                retainedMemoryBytesForStatisticsPages = 0;
            }
        }
    }
}
