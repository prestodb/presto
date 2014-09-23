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
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class TableWriterOperator
        implements Operator
{
    public static final List<Type> TYPES = ImmutableList.<Type>of(BIGINT, VARCHAR);

    public static class TableWriterOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final RecordSink recordSink;
        private final List<Integer> inputChannels;
        private final List<Type> recordTypes;
        private final Optional<Integer> sampleWeightChannel;
        private boolean closed;

        public TableWriterOperatorFactory(int operatorId, RecordSink recordSink, List<Type> recordTypes, List<Integer> inputChannels, Optional<Integer> sampleWeightChannel)
        {
            this.operatorId = operatorId;
            this.inputChannels = checkNotNull(inputChannels, "inputChannels is null");
            this.recordSink = checkNotNull(recordSink, "recordSink is null");

            checkNotNull(recordTypes, "types is null");
            this.recordTypes = ImmutableList.copyOf(Iterables.transform(recordTypes, new Function<Type, Type>()
            {
                @Override
                public Type apply(Type type)
                {
                    return type;
                }
            }));

            this.sampleWeightChannel = checkNotNull(sampleWeightChannel, "sampleWeightChannel is null");
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
            OperatorContext context = driverContext.addOperatorContext(operatorId, TableWriterOperator.class.getSimpleName());
            return new TableWriterOperator(context, recordSink, recordTypes, inputChannels, sampleWeightChannel);
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
    private final Optional<Integer> sampleWeightChannel;
    private final List<Type> recordTypes;
    private final List<Integer> inputChannels;

    private State state = State.RUNNING;
    private long rowCount;

    public TableWriterOperator(OperatorContext operatorContext,
            RecordSink recordSink,
            List<Type> recordTypes,
            List<Integer> inputChannels,
            Optional<Integer> sampleWeightChannel)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.recordSink = checkNotNull(recordSink, "recordSink is null");
        this.recordTypes = recordTypes;
        this.sampleWeightChannel = checkNotNull(sampleWeightChannel, "sampleWeightChannel is null");
        this.inputChannels = checkNotNull(inputChannels, "inputChannels is null");
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

        Block sampleWeightBlock = null;
        if (sampleWeightChannel.isPresent()) {
            sampleWeightBlock = page.getBlock(sampleWeightChannel.get());
        }

        Block[] blocks = new Block[inputChannels.size()];
        for (int outputChannel = 0; outputChannel < inputChannels.size(); outputChannel++) {
            blocks[outputChannel] = page.getBlock(inputChannels.get(outputChannel));
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            long sampleWeight = 1;
            if (sampleWeightBlock != null) {
                sampleWeight = BIGINT.getLong(sampleWeightBlock, position);
            }
            recordSink.beginRecord(sampleWeight);
            for (int i = 0; i < blocks.length; i++) {
                writeField(position, blocks[i], recordTypes.get(i));
            }
            recordSink.finishRecord();
        }
        rowCount += page.getPositionCount();
    }

    private void writeField(int position, Block block, Type type)
    {
        if (block.isNull(position)) {
            recordSink.appendNull();
            return;
        }

        if (type.getJavaType() == boolean.class) {
            recordSink.appendBoolean(type.getBoolean(block, position));
        }
        else if (type.getJavaType() == long.class) {
            recordSink.appendLong(type.getLong(block, position));
        }
        else if (type.getJavaType() == double.class) {
            recordSink.appendDouble(type.getDouble(block, position));
        }
        else if (type.getJavaType() == Slice.class) {
            recordSink.appendString(type.getSlice(block, position).getBytes());
        }
        else {
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

        PageBuilder page = new PageBuilder(TYPES);
        BIGINT.writeLong(page.getBlockBuilder(0), rowCount);
        VARCHAR.writeSlice(page.getBlockBuilder(1), Slices.utf8Slice(fragment));
        return page.build();
    }
}
