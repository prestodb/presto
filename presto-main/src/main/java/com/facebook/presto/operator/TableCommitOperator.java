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

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableCommitOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.<Type>of(BIGINT);

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
        public List<Type> getTypes()
        {
            return TYPES;
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
    private final ImmutableList.Builder<Slice> fragmentBuilder = ImmutableList.builder();

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
    public List<Type> getTypes()
    {
        return TYPES;
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
    public boolean needsInput()
    {
        return state == State.RUNNING;
    }

    @Override
    public void addInput(Page page)
    {
        checkNotNull(page, "page is null");
        checkState(state == State.RUNNING, "Operator is %s", state);

        Block rowCountBlock = page.getBlock(0);
        Block fragmentBlock = page.getBlock(1);
        for (int position = 0; position < page.getPositionCount(); position++) {
            if (!rowCountBlock.isNull(position)) {
                rowCount += BIGINT.getLong(rowCountBlock, position);
            }
            if (!fragmentBlock.isNull(position)) {
                fragmentBuilder.add(VARBINARY.getSlice(fragmentBlock, position));
            }
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

        PageBuilder page = new PageBuilder(getTypes());
        page.declarePosition();
        BIGINT.writeLong(page.getBlockBuilder(0), rowCount);
        return page.build();
    }

    public interface TableCommitter
    {
        void commitTable(Collection<Slice> fragments);
    }
}
