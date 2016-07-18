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
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;

public class ArrayAggregationStateSerializer
        implements AccumulatorStateSerializer<ArrayAggregationState>
{
    private final Type elementType;
    private final Type arrayType;

    public ArrayAggregationStateSerializer(Type elementType)
    {
        this.elementType = elementType;
        this.arrayType = new ArrayType(elementType);
    }

    @Override
    public Type getSerializedType()
    {
        return arrayType;
    }

    @Override
    public void serialize(ArrayAggregationState state, BlockBuilder out)
    {
        if (state.getBlockBuilder() == null) {
            out.appendNull();
        }
        else {
            Block stateBlock = state.getBlockBuilder().build();
            arrayType.writeObject(out, stateBlock);
        }
    }

    @Override
    public void deserialize(Block block, int index, ArrayAggregationState state)
    {
        Block stateBlock = (Block) arrayType.getObject(block, index);
        int positionCount = stateBlock.getPositionCount();
        BlockBuilder blockBuilder = elementType.createBlockBuilder(new BlockBuilderStatus(), positionCount);
        for (int i = 0; i < positionCount; i++) {
            elementType.appendTo(stateBlock, i, blockBuilder);
        }
        state.setBlockBuilder(blockBuilder);
    }
}
