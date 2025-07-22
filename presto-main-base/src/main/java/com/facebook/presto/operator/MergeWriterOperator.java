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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.ConnectorMergeSink;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableWriterNode.MergeTarget;
import com.facebook.presto.split.PageSinkManager;

import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.MoreFutures.toListenableFuture;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MergeWriterOperator
        extends AbstractRowChangeOperator
{
    public static class MergeWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final PageSinkManager pageSinkManager;
        private final MergeTarget target;
        private final Session session;
        private final JsonCodec<TableCommitContext> tableCommitContextCodec;
        private boolean closed;

        public MergeWriterOperatorFactory(
                int operatorId,
                PlanNodeId planNodeId,
                PageSinkManager pageSinkManager,
                MergeTarget target,
                Session session,
                JsonCodec<TableCommitContext> tableCommitContextCodec)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.pageSinkManager = requireNonNull(pageSinkManager, "pageSinkManager is null");
            this.target = requireNonNull(target, "target is null");
            this.session = requireNonNull(session, "session is null");
            this.tableCommitContextCodec = requireNonNull(tableCommitContextCodec, "tableCommitContextCodec is null");
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, planNodeId, MergeWriterOperator.class.getSimpleName());
            ConnectorMergeSink mergeSink = pageSinkManager.createMergeSink(session, target.getMergeHandle().get());
            return new MergeWriterOperator(context, mergeSink, tableCommitContextCodec);
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new MergeWriterOperatorFactory(operatorId, planNodeId, pageSinkManager, target, session, tableCommitContextCodec);
        }
    }

    private final ConnectorMergeSink mergeSink;

    public MergeWriterOperator(OperatorContext operatorContext, ConnectorMergeSink mergeSink, JsonCodec<TableCommitContext> tableCommitContextCodec)
    {
        super(operatorContext, tableCommitContextCodec);
        this.mergeSink = requireNonNull(mergeSink, "mergeSink is null");
    }

    /**
     * @param page It has N + 3 channels/blocks, where N is the number of columns in the source table. <br>
     *                        1: Source table column 1.<br>
     *                        2: Source table column 2.<br>
     *                        N: Source table column N.<br>
     *                        N + 1: Operation: INSERT(1), DELETE(2), UPDATE(3). More info: {@link ConnectorMergeSink}<br>
     *                        N + 2: Merge Row ID (_file:varchar, _pos:bigint, file_record_count:bigint, partition_spec_id:integer, partition_data:varchar).<br>
     *                        N + 3: Insert from update: it is 1 if the cause of the insert is an UPDATE, 0 otherwise.<br>
     */
    @Override
    public void addInput(Page page)
    {
        requireNonNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        // Copy all but the last block to a new page.
        // The last block exists only to get the rowCount right.
        int outputChannelCount = page.getChannelCount() - 1;

        int[] columns = IntStream.range(0, outputChannelCount).toArray();
        Page newPage = page.getColumns(columns);

        // Store the page
        mergeSink.storeMergedRows(newPage);

        // Calculate the amount to increment the rowCount
        Block insertFromUpdateColumn = page.getBlock(page.getChannelCount() - 1);
        long insertsFromUpdates = 0;
        int positionCount = page.getPositionCount();
        for (int position = 0; position < positionCount; position++) {
            insertsFromUpdates += TINYINT.getLong(insertFromUpdateColumn, position);
        }
        rowCount += positionCount - insertsFromUpdates;
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
            finishFuture = toListenableFuture(mergeSink.finish());
        }
    }

    @Override
    protected void abort()
    {
        mergeSink.abort();
    }
}
