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
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.tuple.TupleInfo.Type;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableWriterOperator
        implements Operator
{
    public static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_LONG, SINGLE_VARBINARY);

    public static class TableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final RecordSink recordSink;
        private boolean closed;

        public TableWriterOperatorFactory(int operatorId, RecordSink recordSink)
        {
            this.operatorId = operatorId;
            this.recordSink = checkNotNull(recordSink, "recordSink is null");
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
            OperatorContext context = driverContext.addOperatorContext(operatorId, TableWriterOperator.class.getSimpleName());
            return new TableWriterOperator(context, recordSink);
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
    private final RecordSink recordSink;

    private State state = State.RUNNING;
    private long rowCount;

    public TableWriterOperator(OperatorContext operatorContext, RecordSink recordSink)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.recordSink = checkNotNull(recordSink, "recordSink is null");
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

        BlockCursor[] cursors = new BlockCursor[page.getChannelCount()];
        Type[] types = new Type[page.getChannelCount()];

        for (int channel = 0; channel < cursors.length; channel++) {
            cursors[channel] = page.getBlock(channel).cursor();
            TupleInfo tupleInfo = cursors[channel].getTupleInfo();
            checkArgument(tupleInfo.getFieldCount() == 1, "expected block to have exactly one field");
            types[channel] = tupleInfo.getTypes().get(0);
        }

        int rows = page.getPositionCount();
        for (int position = 0; position < rows; position++) {
            recordSink.beginRecord();
            for (int i = 0; i < cursors.length; i++) {
                checkArgument(cursors[i].advanceNextPosition());
                writeField(cursors[i], types[i]);
            }
            recordSink.finishRecord();
        }
        rowCount += rows;

        for (BlockCursor cursor : cursors) {
            checkArgument(!cursor.advanceNextPosition());
        }
    }

    private void writeField(BlockCursor cursor, Type type)
    {
        if (cursor.isNull(0)) {
            recordSink.setNextNull();
            return;
        }

        switch (type) {
            case BOOLEAN:
                recordSink.setNextBoolean(cursor.getBoolean(0));
                break;
            case FIXED_INT_64:
                recordSink.setNextLong(cursor.getLong(0));
                break;
            case DOUBLE:
                recordSink.setNextDouble(cursor.getDouble(0));
                break;
            case VARIABLE_BINARY:
                recordSink.setNextString(cursor.getSlice(0).getBytes());
                break;
            default:
                throw new AssertionError("unimplemented type: " + type);
        }
    }

    @Override
    public Page getOutput()
    {
        if (state != State.FINISHING) {
            return null;
        }
        state = State.FINISHED;

        String fragment = recordSink.commit();

        PageBuilder page = new PageBuilder(getTupleInfos());
        page.getBlockBuilder(0).append(rowCount);
        page.getBlockBuilder(1).append(fragment);
        return page.build();
    }
}
