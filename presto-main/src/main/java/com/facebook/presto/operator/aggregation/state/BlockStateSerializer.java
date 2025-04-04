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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

public class BlockStateSerializer
        implements AccumulatorStateSerializer<BlockState>
{
    private final Type type;

    public BlockStateSerializer(Type type)
    {
        this.type = type;
    }

    @Override
    public Type getSerializedType()
    {
        return type;
    }

    @Override
    public void serialize(BlockState state, BlockBuilder out)
    {
        if (state.getBlock() == null) {
            out.appendNull();
        }
        else {
            type.writeObject(out, state.getBlock());
        }
    }

    @Override
    public void deserialize(Block block, int index, BlockState state)
    {
        state.setBlock((Block) type.getObject(block, index));
    }
}
