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
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.OperationTimer.OperationTiming;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;

import static com.facebook.presto.SystemSessionProperties.isStatisticsCpuTimerEnabled;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.TableWriterUtils.CONTEXT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.FRAGMENT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.ROW_COUNT_CHANNEL;
import static com.facebook.presto.operator.TableWriterUtils.createStatisticsPage;
import static com.facebook.presto.operator.TableWriterUtils.extractStatisticsRows;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.wrappedBuffer;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;

public class TableWriterMergeOperator
        implements Operator
{
    public static class TableWriterMergeOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final OperatorFactory statisticsAggregationOperatorFactory;
        private final JsonCodec<TableCommitContext> tableCommitContextCodec;
        private final Session session;
        private final List<Type> types;
        private boolean closed;

        public TableWriterMergeOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                OperatorFactory statisticsAggregationOperatorFactory,
                JsonCodec<TableCommitContext> tableCommitContextCodec,
                Session session,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.statisticsAggregationOperatorFactory = requireNonNull(statisticsAggregationOperatorFactory, "statisticsAggregationOperatorFactory is null");
            this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
            this.session = requireNonNull(session, "session is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableWriterMergeOperator.class.getSimpleName());
            Operator statisticsAggregationOperator = statisticsAggregationOperatorFactory.createOperator(driverContext);
            boolean statisticsCpuTimerEnabled = !(statisticsAggregationOperator instanceof DevNullOperator) && isStatisticsCpuTimerEnabled(session);
            return new TableWriterMergeOperator(context, statisticsAggregationOperator, tableCommitContextCodec, statisticsCpuTimerEnabled, types);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableWriterMergeOperatorFactory(operatorId, planNodeId, statisticsAggregationOperatorFactory, tableCommitContextCodec, session, types);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext context;
    private final Operator statisticsAggregationOperator;
    private final JsonCodec<TableCommitContext> tableCommitContextCodec;
    private final LocalMemoryContext systemMemoryContext;
    private final List<Type> types;

    private final OperationTiming statisticsTiming = new OperationTiming();
    private final boolean statisticsCpuTimerEnabled;

    private TableCommitContext lastTableCommitContext;
    private long rowCount;
    private final Queue<Block> fragmentsBlocks = new LinkedList<>();

    private State state = State.RUNNING;

    public TableWriterMergeOperator(
            OperatorContext context,
            Operator statisticsAggregationOperator,
            JsonCodec<TableCommitContext> tableCommitContextCodec,
            boolean statisticsCpuTimerEnabled,
            List<Type> types)
    {
        this.context = requireNonNull(context, "context is null");
        this.statisticsAggregationOperator = requireNonNull(statisticsAggregationOperator, "statisticAggregationOperator is null");
        this.systemMemoryContext = context.newLocalSystemMemoryContext(InMemoryHashAggregationBuilder.class.getSimpleName());
        this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        this.statisticsCpuTimerEnabled = statisticsCpuTimerEnabled;
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.context.setInfoSupplier(createTableWriterMergeInfoSupplier(statisticsTiming));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return context;
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

        // make sure the lifespan is the same
        TableCommitContext tableCommitContext = TableWriterUtils.getTableCommitContext(page, tableCommitContextCodec);
        if (lastTableCommitContext != null) {
            checkArgument(
                    isSameTaskAndLifespan(lastTableCommitContext, tableCommitContext),
                    "incompatible table commit context: %s is not compatible with %s",
                    lastTableCommitContext,
                    tableCommitContext);
        }
        lastTableCommitContext = tableCommitContext;

        // increment rows
        rowCount += getRowCount(page);

        // Add fragments to the buffer.
        // Fragments will be outputted as soon as possible to avoid using extra memory.
        Block fragmentsBlock = page.getBlock(FRAGMENT_CHANNEL);
        if (containsNonNullRows(fragmentsBlock)) {
            fragmentsBlocks.add(fragmentsBlock);
        }

        extractStatisticsRows(page).ifPresent(statisticsPage -> {
            OperationTimer timer = new OperationTimer(statisticsCpuTimerEnabled);
            statisticsAggregationOperator.addInput(statisticsPage);
            timer.end(statisticsTiming);
        });

        systemMemoryContext.setBytes(getRetainedMemoryBytes());
    }

    private static long getRowCount(Page page)
    {
        long rowCount = 0;
        Block rowCountBlock = page.getBlock(ROW_COUNT_CHANNEL);
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (!rowCountBlock.isNull(position)) {
                rowCount += BIGINT.getLong(rowCountBlock, position);
            }
        }
        return rowCount;
    }

    private static boolean containsNonNullRows(Block block)
    {
        // shortcut for RunLengthEncodedBlock
        if (block instanceof RunLengthEncodedBlock) {
            RunLengthEncodedBlock runLengthEncodedBlock = (RunLengthEncodedBlock) block;
            return !runLengthEncodedBlock.getValue().isNull(0);
        }
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isSameTaskAndLifespan(TableCommitContext first, TableCommitContext second)
    {
        return first.getLifespan().equals(second.getLifespan()) &&
                first.getTaskId().equals(second.getTaskId()) &&
                first.getPageSinkCommitStrategy() == second.getPageSinkCommitStrategy();
    }

    private long getRetainedMemoryBytes()
    {
        return fragmentsBlocks.stream().mapToLong(Block::getRetainedSizeInBytes).sum();
    }

    @Override
    public Page getOutput()
    {
        // pass through fragment pages first to avoid use extra memory
        if (!fragmentsBlocks.isEmpty()) {
            Block fragmentsBlock = fragmentsBlocks.poll();
            systemMemoryContext.setBytes(getRetainedMemoryBytes());
            return createFragmentsPage(fragmentsBlock);
        }

        // still working on merging statistic pages
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

            return createStatisticsPage(types, page, createTableCommitContext(false));
        }

        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        Page finalPage = createFinalPage();
        systemMemoryContext.setBytes(getRetainedMemoryBytes());
        return finalPage;
    }

    private Page createFragmentsPage(Block fragmentsBlock)
    {
        int positionCount = fragmentsBlock.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel == FRAGMENT_CHANNEL) {
                outputBlocks[channel] = fragmentsBlock;
            }
            else if (channel == CONTEXT_CHANNEL) {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), createTableCommitContext(false), positionCount);
            }
            else {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
        }
        return new Page(outputBlocks);
    }

    private Page createFinalPage()
    {
        checkState(lastTableCommitContext.isLastPage(), "unexpected last table commit context: %s", lastTableCommitContext);
        PageBuilder pageBuilder = new PageBuilder(1, types);
        pageBuilder.declarePosition();
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel == ROW_COUNT_CHANNEL) {
                types.get(channel).writeLong(pageBuilder.getBlockBuilder(channel), rowCount);
            }
            else if (channel == CONTEXT_CHANNEL) {
                types.get(channel).writeSlice(pageBuilder.getBlockBuilder(channel), createTableCommitContext(true));
            }
            else {
                pageBuilder.getBlockBuilder(channel).appendNull();
            }
        }
        return pageBuilder.build();
    }

    private Slice createTableCommitContext(boolean lastPage)
    {
        checkState(tableCommitContextCodec != null, "tableCommitContextCodec is null");
        return wrappedBuffer(tableCommitContextCodec.toJsonBytes(new TableCommitContext(
                lastTableCommitContext.getLifespan(),
                lastTableCommitContext.getTaskId(),
                lastTableCommitContext.getPageSinkCommitStrategy(),
                lastPage)));
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
    public void close()
            throws Exception
    {
        statisticsAggregationOperator.close();
        systemMemoryContext.setBytes(0);
    }

    private static Supplier<TableWriterMergeInfo> createTableWriterMergeInfoSupplier(OperationTiming statisticsTiming)
    {
        requireNonNull(statisticsTiming, "statisticsTiming is null");
        return () -> new TableWriterMergeInfo(
                succinctNanos(statisticsTiming.getWallNanos()),
                succinctNanos(statisticsTiming.getCpuNanos()));
    }
}
