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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.window.SplitBlockedReason;
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.operator.PageSinkCommitStrategy.NO_COMMIT;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;

public class DeleteOperator
        implements Operator
{
    public static class DeleteOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final int rowIdChannel;
        private final JsonCodec<TableCommitContext> tableCommitContextCodec;
        private boolean closed;

        public DeleteOperatorFactory(int operatorId, PlanNodeId planNodeId, int rowIdChannel, JsonCodec<TableCommitContext> tableCommitContextCodec)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.rowIdChannel = rowIdChannel;
            this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, DeleteOperator.class.getSimpleName());
            return new DeleteOperator(context, rowIdChannel, tableCommitContextCodec);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new DeleteOperatorFactory(operatorId, planNodeId, rowIdChannel, tableCommitContextCodec);
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final int rowIdChannel;
    private final JsonCodec<TableCommitContext> tableCommitContextCodec;

    private State state = State.RUNNING;
    private long rowCount;
    private boolean closed;
    private ListenableFuture<Collection<Slice>> finishFuture;
    private Supplier<Optional<UpdatablePageSource>> pageSource = Optional::empty;

    public DeleteOperator(OperatorContext operatorContext, int rowIdChannel, JsonCodec<TableCommitContext> tableCommitContextCodec)
    {
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        this.rowIdChannel = rowIdChannel;
        this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
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
            finishFuture = toListenableFuture(pageSource().finish());
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING;
    }

    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        Block rowIds = page.getBlock(rowIdChannel);
        pageSource().deleteRows(rowIds);
        rowCount += rowIds.getPositionCount();
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        if (finishFuture == null) {
            return NOT_BLOCKED;
        }
        return new Driver.BlockedFuture(finishFuture, SplitBlockedReason.DELETE);
    }

    @Override
    public Page getOutput()
    {
        if ((state != State.FINISHING) || !finishFuture.isDone()) {
            return null;
        }
        state = State.FINISHED;

        // There are three channels in the output page of DeleteOperator
        // 1. Row count (BIGINT)
        // 2. Delete fragments (VARBINARY)
        // 3. Table commit context (VARBINARY)
        //
        // Page layout:
        //
        // row     fragments     context
        //  X         null          X
        // null        X            X
        // null        X            X
        // null        X            X
        // ...
        Collection<Slice> fragments = getFutureValue(finishFuture);
        int positionCount = fragments.size() + 1;

        // Output page will only be constructed once, and the table commit context channel will be constructed using RunLengthEncodedBlock.
        // Thus individual BlockBuilder is used for each channel, instead of using PageBuilder.
        BlockBuilder rowsBuilder = BIGINT.createBlockBuilder(null, positionCount);
        BlockBuilder fragmentBuilder = VARBINARY.createBlockBuilder(null, positionCount);

        // write row count
        rowsBuilder.writeLong(rowCount);
        fragmentBuilder.appendNull();

        // write fragments
        for (Slice fragment : fragments) {
            rowsBuilder.appendNull();
            VARBINARY.writeSlice(fragmentBuilder, fragment);
        }

        // create table commit context
        TaskId taskId = operatorContext.getDriverContext().getPipelineContext().getTaskId();
        Slice tableCommitContext = wrappedBuffer(tableCommitContextCodec.toJsonBytes(
                new TableCommitContext(
                        operatorContext.getDriverContext().getLifespan(),
                        taskId,
                        NO_COMMIT,
                        true)));

        return new Page(positionCount, rowsBuilder.build(), fragmentBuilder.build(), RunLengthEncodedBlock.create(VARBINARY, tableCommitContext, positionCount));
    }

    @Override
    public void close()
    {
        if (!closed) {
            closed = true;
            if (finishFuture != null) {
                finishFuture.cancel(true);
            }
            else {
                pageSource.get().ifPresent(UpdatablePageSource::abort);
            }
        }
    }

    public void setPageSource(Supplier<Optional<UpdatablePageSource>> pageSource)
    {
        this.pageSource = requireNonNull(pageSource, "pageSource is null");
    }

    private UpdatablePageSource pageSource()
    {
        Optional<UpdatablePageSource> source = pageSource.get();
        checkState(source.isPresent(), "UpdatablePageSource not set");
        return source.get();
    }
}
