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

import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableCommitOperator
        implements Operator
{
    public static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG);

    public static class TableCommitOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final TableCommitter tableCommitter;
        private boolean closed;

        public TableCommitOperatorFactory(int operatorId, TableCommitter tableCommitter)
        {
            this.operatorId = operatorId;
            this.tableCommitter = checkNotNull(tableCommitter, "tableCommitter is null");
        }

        @Override
        public List<TupleInfo> getTupleInfos()
        {
            return TUPLE_INFOS;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext context = driverContext.addOperatorContext(operatorId, TableCommitOperator.class.getSimpleName());
            return new TableCommitOperator(context, tableCommitter);
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }

    private enum State
    {
        RUNNING, FINISHING, FINISHED
    }

    private final OperatorContext operatorContext;
    private final TableCommitter tableCommitter;

    private State state = State.RUNNING;
    private long rowCount;
    private final ImmutableList.Builder<String> fragmentBuilder = ImmutableList.builder();

    public TableCommitOperator(OperatorContext operatorContext, TableCommitter tableCommitter)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.tableCommitter = checkNotNull(tableCommitter, "tableCommitter is null");
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return TUPLE_INFOS;
    }

    @Override
    public void finish()
    {
        if (state == State.RUNNING) {
            state = State.FINISHING;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }

    @Override
    public ListenableFuture<?> isBlocked()
    {
        return NOT_BLOCKED;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.RUNNING;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        BlockCursor rowCountCursor = page.getBlock(0).cursor();
        BlockCursor fragmentCursor = page.getBlock(1).cursor();
        for (int i = 0; i < page.getPositionCount(); i++) {
            checkArgument(rowCountCursor.advanceNextPosition());
            checkArgument(fragmentCursor.advanceNextPosition());
            rowCount += rowCountCursor.getLong(0);
            fragmentBuilder.add(fragmentCursor.getSlice(0).toStringUtf8());
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        tableCommitter.commitTable(fragmentBuilder.build());

        PageBuilder page = new PageBuilder(getTupleInfos());
        page.getBlockBuilder(0).append(rowCount);
        return page.build();
    }

    public interface TableCommitter
    {
        void commitTable(Collection<String> fragments);
    }
}
