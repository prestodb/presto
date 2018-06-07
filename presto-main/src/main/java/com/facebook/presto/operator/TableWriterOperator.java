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
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.PageSinkManager;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
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

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.CreateHandle;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.InsertHandle;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static java.util.Objects.requireNonNull;

public class TableWriterOperator
        implements Operator
{
    private static final int ROW_COUNT_CHANNEL = 0;
    private static final int FRAGMENT_CHANNEL = 1;

    public static class TableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final WriterTarget target;
        private final List<Integer> columnChannels;
        private final Session session;
        private final OperatorFactory statisticsAggregation;
        private final List<Type> types;
        private boolean closed;

        public TableWriterOperatorFactory(int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                WriterTarget writerTarget,
                List<Integer> columnChannels,
                Session session,
                OperatorFactory statisticsAggregation,
                List<Type> types)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            checkArgument(writerTarget instanceof CreateHandle || writerTarget instanceof InsertHandle, "writerTarget must be CreateHandle or InsertHandle");
            this.target = requireNonNull(writerTarget, "writerTarget is null");
            this.session = session;
            this.statisticsAggregation = requireNonNull(statisticsAggregation, "statisticsAggregation is null");
            this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, TableWriterOperator.class.getSimpleName());
            return new TableWriterOperator(context, createPageSink(), columnChannels, statisticsAggregation.createOperator(driverContext), types);
        }

        private ConnectorPageSink createPageSink()
        {
            if (target instanceof CreateHandle) {
                return pageSinkManager.createPageSink(session, ((CreateHandle) target).getHandle());
            }
            if (target instanceof InsertHandle) {
                return pageSinkManager.createPageSink(session, ((InsertHandle) target).getHandle());
            }
            throw new UnsupportedOperationException("Unhandled target type: " + target.getClass().getName());
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new TableWriterOperatorFactory(operatorId, planNodeId, pageSinkManager, target, columnChannels, session, statisticsAggregation, types);
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
    private final AtomicLong pageSinkPeakMemoryUsage = new AtomicLong();
    private final Operator statisticAggregation;
    private final List<Type> types;

    private ListenableFuture<?> blocked = NOT_BLOCKED;
    private CompletableFuture<Collection<Slice>> finishFuture;
    private State state = State.RUNNING;
    private long rowCount;
    private boolean committed;
    private boolean closed;
    private long writtenBytes;

    public TableWriterOperator(
            OperatorContext operatorContext,
            ConnectorPageSink pageSink,
            List<Integer> columnChannels,
            Operator statisticAggregation,
            List<Type> types)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.pageSinkMemoryContext = operatorContext.newLocalSystemMemoryContext();
        this.pageSink = requireNonNull(pageSink, "pageSink is null");
        this.columnChannels = requireNonNull(columnChannels, "columnChannels is null");
        this.operatorContext.setInfoSupplier(this::getInfo);
        this.statisticAggregation = requireNonNull(statisticAggregation, "statisticAggregation is null");
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = pageSink.finish();
            blocked = toListenableFuture(finishFuture);
            updateWrittenBytes();
            statisticAggregation.finish();
        }
    }

    @Override
    public boolean isFinished()
    {
        updateBlockedIfNecessary();
        return state == State.FINISHED && blocked == NOT_BLOCKED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        updateBlockedIfNecessary();
        return blocked;
    }

    @Override
    public boolean needsInput()
    {
        updateBlockedIfNecessary();
        return state == State.RUNNING && blocked == NOT_BLOCKED && statisticAggregation.needsInput();
    }

    private void updateBlockedIfNecessary()
    {
        if (blocked != NOT_BLOCKED && blocked.isDone()) {
            blocked = statisticAggregation.isBlocked();
        }
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(needsInput(), "Operator does not need input");

        Block[] blocks = new Block[columnChannels.size()];
        for (int outputChannel = 0; outputChannel < columnChannels.size(); outputChannel++) {
            blocks[outputChannel] = page.getBlock(columnChannels.get(outputChannel));
        }

        statisticAggregation.addInput(page);
        CompletableFuture<?> future = pageSink.appendPage(new Page(blocks));
        updateMemoryUsage();
        if (!future.isDone()) {
            this.blocked = toListenableFuture(future);
        }
        rowCount += page.getPositionCount();
        updateWrittenBytes();
    }

    @Override
    public Page getOutput()
    {
        if (!blocked.isDone()) {
            return null;
        }

        if (!statisticAggregation.isFinished()) {
            Page aggregationOutput = statisticAggregation.getOutput();
            if (aggregationOutput == null) {
                return null;
            }
            int positionCount = aggregationOutput.getPositionCount();
            Block[] outputBlocks = new Block[types.size()];
            for (int channel = 0; channel < types.size(); channel++) {
                if (channel == ROW_COUNT_CHANNEL || channel == FRAGMENT_CHANNEL) {
                    outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
                }
                else {
                    outputBlocks[channel] = aggregationOutput.getBlock(channel - 2);
                }
            }
            return new Page(positionCount, outputBlocks);
        }

        if (state != State.FINISHING) {
            return null;
        }

        state = State.FINISHED;

        Page fragmentsPage = createFragmentsPage();
        int positionCount = fragmentsPage.getPositionCount();
        Block[] outputBlocks = new Block[types.size()];
        for (int channel = 0; channel < types.size(); channel++) {
            if (channel == ROW_COUNT_CHANNEL || channel == FRAGMENT_CHANNEL) {
                outputBlocks[channel] = fragmentsPage.getBlock(channel);
            }
            else {
                outputBlocks[channel] = RunLengthEncodedBlock.create(types.get(channel), null, positionCount);
            }
        }
        return new Page(positionCount, outputBlocks);
    }

    private Page createFragmentsPage()
    {
        Collection<Slice> fragments = getFutureValue(finishFuture);
        committed = true;
        updateWrittenBytes();

        // output page will only be constructed once,
        // so a new PageBuilder is constructed (instead of using PageBuilder.reset)
        PageBuilder page = new PageBuilder(fragments.size() + 1, ImmutableList.of(types.get(ROW_COUNT_CHANNEL), types.get(FRAGMENT_CHANNEL)));
        BlockBuilder rowsBuilder = page.getBlockBuilder(0);
        BlockBuilder fragmentBuilder = page.getBlockBuilder(1);

        // write row count
        page.declarePosition();
        BIGINT.writeLong(rowsBuilder, rowCount);
        fragmentBuilder.appendNull();

        // write fragments
        for (Slice fragment : fragments) {
            page.declarePosition();
            rowsBuilder.appendNull();
            VARBINARY.writeSlice(fragmentBuilder, fragment);
        }

        return page.build();
    }

    @Override
    public void close()
            throws Exception
    {
        if (!closed) {
            closed = true;
            if (!committed) {
                pageSink.abort();
            }
            statisticAggregation.close();
        }
    }

    private void updateWrittenBytes()
    {
        long current = pageSink.getCompletedBytes();
        operatorContext.recordPhysicalWrittenData(current - writtenBytes);
        writtenBytes = current;
    }

    private void updateMemoryUsage()
    {
        long pageSinkMemoryUsage = pageSink.getSystemMemoryUsage();
        pageSinkMemoryContext.setBytes(pageSinkMemoryUsage);
        pageSinkPeakMemoryUsage.accumulateAndGet(pageSinkMemoryUsage, Math::max);
    }

    @VisibleForTesting
    TableWriterInfo getInfo()
    {
        return new TableWriterInfo(pageSinkPeakMemoryUsage.get());
    }

    public static class TableWriterInfo
            implements Mergeable<TableWriterInfo>, OperatorInfo
    {
        private final long pageSinkPeakMemoryUsage;

        @JsonCreator
        public TableWriterInfo(@JsonProperty("pageSinkPeakMemoryUsage") long pageSinkPeakMemoryUsage)
        {
            this.pageSinkPeakMemoryUsage = pageSinkPeakMemoryUsage;
        }

        @JsonProperty
        public long getPageSinkPeakMemoryUsage()
        {
            return pageSinkPeakMemoryUsage;
        }

        @Override
        public TableWriterInfo mergeWith(TableWriterInfo other)
        {
            return new TableWriterInfo(Math.max(pageSinkPeakMemoryUsage, other.pageSinkPeakMemoryUsage));
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
                    .toString();
        }
    }
}
