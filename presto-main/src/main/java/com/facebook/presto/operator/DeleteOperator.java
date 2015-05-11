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
import com.facebook.presto.spi.UpdatablePageSource;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DeleteOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.<Type>of(BIGINT, VARBINARY);

    public static class DeleteOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final int rowIdChannel;
        private boolean closed;

        public DeleteOperatorFactory(int operatorId, int rowIdChannel)
        {
            this.operatorId = operatorId;
            this.rowIdChannel = rowIdChannel;
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
            OperatorContext context = driverContext.addOperatorContext(operatorId, DeleteOperator.class.getSimpleName());
            return new DeleteOperator(context, rowIdChannel);
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
    private final int rowIdChannel;

    private State state = State.RUNNING;
    private long rowCount;
    private boolean committed;
    private boolean closed;
    private Supplier<Optional<UpdatablePageSource>> pageSource = Optional::empty;

    public DeleteOperator(OperatorContext operatorContext, int rowIdChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.rowIdChannel = rowIdChannel;
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

        Block rowIds = page.getBlock(rowIdChannel);
        pageSource().deleteRows(rowIds);
        rowCount += rowIds.getPositionCount();
    }

    @Override
    public Page getOutput()
    {
        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        Collection<Slice> fragments = pageSource().commit();
        committed = true;

        PageBuilder page = new PageBuilder(TYPES);
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
                pageSource.get().ifPresent(UpdatablePageSource::rollback);
            }
        }
    }

    public void setPageSource(Supplier<Optional<UpdatablePageSource>> pageSource)
    {
        this.pageSource = checkNotNull(pageSource, "pageSource is null");
    }

    private UpdatablePageSource pageSource()
    {
        Optional<UpdatablePageSource> source = pageSource.get();
        checkState(source.isPresent(), "UpdatablePageSource not set");
        return source.get();
    }
}
