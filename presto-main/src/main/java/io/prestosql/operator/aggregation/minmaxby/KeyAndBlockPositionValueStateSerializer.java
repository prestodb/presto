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
package io.prestosql.operator.aggregation.minmaxby;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.block.AbstractRowBlock;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.block.ColumnarRow;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;

import static com.google.common.base.Preconditions.checkArgument;
import static io.prestosql.spi.block.ColumnarRow.toColumnarRow;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
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
        return RowType.anonymous(ImmutableList.of(BOOLEAN, BOOLEAN, firstType, secondType));
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
        checkArgument(block instanceof AbstractRowBlock);
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
