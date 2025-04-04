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
import com.facebook.presto.common.type.Type;

class BlockAndBlockPositionStateSerializer
        extends KeyAndBlockPositionValueStateSerializer<BlockAndBlockPositionValueState>
{
    BlockAndBlockPositionStateSerializer(Type firstType, Type secondType)
    {
        super(firstType, secondType);
    }

    @Override
    void readFirstField(Block block, int position, BlockAndBlockPositionValueState state)
    {
        state.setFirst((Block) firstType.getObject(block, position));
    }

    @Override
    void writeFirstField(BlockBuilder out, BlockAndBlockPositionValueState state)
    {
        firstType.writeObject(out, state.getFirst());
    }
}
