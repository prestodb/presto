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

import com.facebook.presto.Session;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.operator.OperationTimer.OperationTiming;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static com.facebook.presto.operator.TableWriterUtils.FRAGMENT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.ROW_COUNT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.extractStatisticsRows;
import static com.facebook.presto.operator.TableWriterUtils.getTableCommitContext;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.Duration.succinctNanos;
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
        private final LifespanCommitter lifespanCommitter;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final StatisticAggregationsDescriptor<Integer> descriptor;
        private final Session session;
        private final JsonCodec<TableCommitContext> tableCommitContextCodec;

        private boolean closed;

        public TableFinishOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                TableFinisher tableFinisher,
                LifespanCommitter lifespanCommitter,
                OperatorFactory statisticsAggregationOperatorFactory,
                StatisticAggregationsDescriptor<Integer> descriptor,
                Session session,
                JsonCodec<TableCommitContext> tableCommitContextCodec)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.tableFinisher = requireNonNull(tableFinisher, "tableFinisher is null");
            this.lifespanCommitter = requireNonNull(lifespanCommitter, "lifespanCommitter is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.descriptor = requireNonNull(descriptor, "descriptor is null");
            this.session = requireNonNull(session, "session is null");
            this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableFinishOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableFinishOperator(context, tableFinisher, lifespanCommitter, statisticsAggregationOperator, descriptor, statisticsCpuTimerEnabled, tableCommitContextCodec);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableFinishOperatorFactory(operatorId, planNodeId, tableFinisher, lifespanCommitter, statisticsAggregationOperatorFactory, descriptor, session, tableCommitContextCodec);
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
    private Optional<ConnectorOutputMetadata> outputMetadata = Optional.empty();
    private final ImmutableList.Builder<ComputedStatistics> computedStatisticsBuilder = ImmutableList.builder();

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private final JsonCodec<TableCommitContext> tableCommitContextCodec;
    private final LifespanAndStageStateTracker lifespanAndStageStateTracker;

    public TableFinishOperator(
            OperatorContext operatorContext,
            TableFinisher tableFinisher,
            LifespanCommitter lifespanCommitter,
            Operator statisticsAggregationOperator,
            StatisticAggregationsDescriptor<Integer> descriptor,
            boolean statisticsCpuTimerEnabled,
            JsonCodec<TableCommitContext> tableCommitContextCodec)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.tableFinisher = requireNonNull(tableFinisher, "tableCommitter is null");
        this.statisticsAggregationOperator = requireNonNull(statisticsAggregationOperator, "statisticsAggregationOperator is null");
        this.descriptor = requireNonNull(descriptor, "descriptor is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        this.lifespanAndStageStateTracker = new LifespanAndStageStateTracker(lifespanCommitter);

        operatorContext.setInfoSupplier(this::getInfo);
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

        outputMetadata = tableFinisher.finishTable(lifespanAndStageStateTracker.getFinalFragments(), computedStatisticsBuilder.build());

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
        return new TableFinishInfo(
                outputMetadata,
                succinctNanos(statisticsTiming.getWallNanos()),
                succinctNanos(statisticsTiming.getCpuNanos()));
    }

    @Override
    public void close()
            throws Exception
    {
        statisticsAggregationOperator.close();
    }

    public interface TableFinisher
    {
        Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics);
    }

    public interface LifespanCommitter
    {
        void commitLifespan(Collection<Slice> fragments);
    }

    // A lifespan in a stage defines the unit for commit and recovery in recoverable grouped execution
    private static class LifespanAndStageStateTracker
    {
        private final Map<LifespanAndStage, LifespanAndStageState> unrecoverableLifespanAndStageStates = new HashMap<>();

        // For recoverable execution, it is possible to receive pages of the same lifespan-stage from different tasks. We track all of them and commit the one
        // which finishes sending pages first.
        private final Map<LifespanAndStage, Map<Integer, LifespanAndStageState>> uncommittedRecoverableLifespanAndStageStates = new HashMap<>();
        private final Map<LifespanAndStage, LifespanAndStageState> committedRecoverableLifespanAndStages = new HashMap<>();

        private final LifespanCommitter lifespanCommitter;

        LifespanAndStageStateTracker(LifespanCommitter lifespanCommitter)
        {
            this.lifespanCommitter = requireNonNull(lifespanCommitter, "lifespanCommitter is null");
        }

        public void update(Page page, TableCommitContext tableCommitContext)
        {
            LifespanAndStage lifespanAndStage = LifespanAndStage.fromTableCommitContext(tableCommitContext);
            if (committedRecoverableLifespanAndStages.containsKey(lifespanAndStage)) {
                return;
            }

            if (!tableCommitContext.isLifespanCommitRequired()) {
                unrecoverableLifespanAndStageStates.computeIfAbsent(lifespanAndStage, ignored -> new LifespanAndStageState()).update(page);
                return;
            }

            Map<Integer, LifespanAndStageState> lifespanStageStatesPerTask = uncommittedRecoverableLifespanAndStageStates.computeIfAbsent(lifespanAndStage, ignored -> new HashMap<>());
            lifespanStageStatesPerTask.computeIfAbsent(tableCommitContext.getTaskId(), ignored -> new LifespanAndStageState()).update(page);

            if (tableCommitContext.isLastPage()) {
                checkState(!committedRecoverableLifespanAndStages.containsKey(lifespanAndStage), "LifespanAndStage already finished");
                LifespanAndStageState lifespanAndStageState = lifespanStageStatesPerTask.get(tableCommitContext.getTaskId());
                committedRecoverableLifespanAndStages.put(lifespanAndStage, lifespanAndStageState);
                uncommittedRecoverableLifespanAndStageStates.remove(lifespanAndStage);
                lifespanCommitter.commitLifespan(lifespanAndStageState.getFragments());
            }
        }

        List<Page> getStatisticsPagesToProcess(Page page, TableCommitContext tableCommitContext)
        {
            LifespanAndStage lifespanAndStage = LifespanAndStage.fromTableCommitContext(tableCommitContext);
            if (!tableCommitContext.isLifespanCommitRequired()) {
                return extractStatisticsRows(page).map(ImmutableList::of).orElse(ImmutableList.of());
            }
            if (!committedRecoverableLifespanAndStages.containsKey(lifespanAndStage)) {
                return ImmutableList.of();
            }
            checkState(!uncommittedRecoverableLifespanAndStageStates.containsKey(lifespanAndStage), "lifespanAndStage %s is already committed", lifespanAndStage);
            return committedRecoverableLifespanAndStages.get(lifespanAndStage).getStatisticsPages();
        }

        public long getFinalRowCount()
        {
            checkState(uncommittedRecoverableLifespanAndStageStates.isEmpty(), "All recoverable LifespanAndStage should be committed when fetching final row count");
            return Stream.concat(unrecoverableLifespanAndStageStates.values().stream(), committedRecoverableLifespanAndStages.values().stream())
                    .mapToLong(LifespanAndStageState::getRowCount)
                    .sum();
        }

        public List<Slice> getFinalFragments()
        {
            checkState(uncommittedRecoverableLifespanAndStageStates.isEmpty(), "All recoverable LifespanAndStage should be committed when fetching final fragments");
            return Stream.concat(unrecoverableLifespanAndStageStates.values().stream(), committedRecoverableLifespanAndStages.values().stream())
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
                return new LifespanAndStage(operatorExecutionContext.getLifespan(), operatorExecutionContext.getStageId());
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
            private long rowCount;
            private ImmutableList.Builder<Slice> fragmentBuilder = ImmutableList.builder();
            private ImmutableList.Builder<Page> statisticsPages = ImmutableList.builder();

            public void update(Page page)
            {
                Block rowCountBlock = page.getBlock(ROW_COUNT_CHANNEL);
                Block fragmentBlock = page.getBlock(FRAGMENT_CHANNEL);
                for (int position = 0; position < page.getPositionCount(); position++) {
                    if (!rowCountBlock.isNull(position)) {
                        rowCount += BIGINT.getLong(rowCountBlock, position);
                    }
                    if (!fragmentBlock.isNull(position)) {
                        fragmentBuilder.add(VARBINARY.getSlice(fragmentBlock, position));
                    }
                }
                extractStatisticsRows(page).ifPresent(statisticsPages::add);
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
                return statisticsPages.build();
            }
        }
    }
}
