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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class MaxOrMinByStateSerializer
        implements AccumulatorStateSerializer<MaxOrMinByState>
{
    private final Type valueType;
    private final Type keyType;
    private final Type serializedType;

    public MaxOrMinByStateSerializer(Type valueType, Type keyType)
    {
        this.valueType = valueType;
        this.keyType = keyType;
        this.serializedType = new RowType(ImmutableList.of(keyType, valueType), Optional.empty());
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MaxOrMinByState state, BlockBuilder out)
    {
        Block keyState = state.getKey();
        Block valueState = state.getValue();

        checkState((keyState == null) == (valueState == null), "(keyState == null) != (valueState == null)");
        if (keyState == null) {
            out.appendNull();
            return;
        }

        BlockBuilder blockBuilder = out.beginBlockEntry();
        keyType.appendTo(keyState, 0, blockBuilder);
        valueType.appendTo(valueState, 0, blockBuilder);
        out.closeEntry();
    }

    @Override
    public void deserialize(Block block, int index, MaxOrMinByState state)
    {
        Block rowBlock = block.getObject(index, Block.class);
        state.setKey(rowBlock.getSingleValueBlock(0));
        state.setValue(rowBlock.getSingleValueBlock(1));
    }
}
