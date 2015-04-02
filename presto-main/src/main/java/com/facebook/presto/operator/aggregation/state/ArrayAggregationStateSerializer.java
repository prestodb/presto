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
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;

public class ArrayAggregationStateSerializer
        implements AccumulatorStateSerializer<ArrayAggregationState>
{
    private final Type type;

    public ArrayAggregationStateSerializer(Type type)
    {
        this.type = type;
    }

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(ArrayAggregationState state, BlockBuilder out)
    {
        if (state.getBlockBuilder() == null) {
            out.appendNull();
        }
        else {
            BlockBuilder stateBlockBuilder = state.getBlockBuilder();
            VARBINARY.writeSlice(out, buildStructuralSlice(stateBlockBuilder));
        }
    }

    @Override
    public void deserialize(Block block, int index, ArrayAggregationState state)
    {
        Slice slice = VARBINARY.getSlice(block, index);
        Block stateBlock = readStructuralBlock(slice);
        BlockBuilder stateBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus()); // not using type.createBlockBuilder because fixedWidth ones can't be serialized
        for (int i = 0; i < stateBlock.getPositionCount(); i++) {
            type.appendTo(stateBlock, i, stateBlockBuilder);
        }
        state.setBlockBuilder(stateBlockBuilder);
    }
}
