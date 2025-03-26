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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

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
        if (state.isEmpty()) {
            out.appendNull();
        }
        else {
            BlockBuilder entryBuilder = out.beginBlockEntry();
            state.forEach((block, position) -> elementType.appendTo(block, position, entryBuilder));
            out.closeEntry();
        }
    }

    @Override
    public void deserialize(Block block, int index, ArrayAggregationState state)
    {
        state.reset();
        Block stateBlock = (Block) arrayType.getObject(block, index);
        for (int i = 0; i < stateBlock.getPositionCount(); i++) {
            state.add(stateBlock, i);
        }
    }
}
