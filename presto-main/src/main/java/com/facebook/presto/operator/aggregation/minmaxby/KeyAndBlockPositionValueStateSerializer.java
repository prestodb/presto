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
package com.facebook.presto.operator.aggregation.minmaxby;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;

public abstract class KeyAndBlockPositionValueStateSerializer<T extends KeyAndBlockPositionValueState>
        implements AccumulatorStateSerializer<T>
{
    final Type firstType;
    protected final Type secondType;

    abstract void readFirstField(Block block, int index, T state);

    abstract void writeFirstField(BlockBuilder out, T state);

    KeyAndBlockPositionValueStateSerializer(Type firstType, Type secondType)
    {
        this.firstType = requireNonNull(firstType, "firstType is null");
        this.secondType = requireNonNull(secondType, "secondType is null");
    }

    @Override
    public Type getSerializedType()
    {
        // Types are: firstNull, secondNull, firstField, secondField
        return RowType.withDefaultFieldNames(ImmutableList.of(BOOLEAN, BOOLEAN, firstType, secondType));
    }

    @Override
    public void serialize(T state, BlockBuilder out)
    {
        BlockBuilder blockBuilder = out.beginBlockEntry();
        BOOLEAN.writeBoolean(blockBuilder, state.isFirstNull());
        BOOLEAN.writeBoolean(blockBuilder, state.isSecondNull());

        if (state.isFirstNull()) {
            blockBuilder.appendNull();
        }
        else {
            writeFirstField(blockBuilder, state);
        }

        if (state.isSecondNull()) {
            blockBuilder.appendNull();
        }
        else {
            secondType.appendTo(state.getSecondBlock(), state.getSecondPosition(), blockBuilder);
        }
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, T state)
    {
        ColumnarRow columnarRow = toColumnarRow(block);

        state.setFirstNull(BOOLEAN.getBoolean(columnarRow.getField(0), index));
        state.setSecondNull(BOOLEAN.getBoolean(columnarRow.getField(1), index));

        if (!state.isFirstNull()) {
            readFirstField(columnarRow.getField(2), index, state);
        }

        if (!state.isSecondNull()) {
            state.setSecondPosition(index);
            state.setSecondBlock(columnarRow.getField(3));
        }
    }
}
