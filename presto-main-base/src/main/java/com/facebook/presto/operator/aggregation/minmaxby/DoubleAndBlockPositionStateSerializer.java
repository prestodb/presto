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

class DoubleAndBlockPositionStateSerializer
        extends KeyAndBlockPositionValueStateSerializer<DoubleAndBlockPositionValueState>
{
    DoubleAndBlockPositionStateSerializer(Type firstType, Type secondType)
    {
        super(firstType, secondType);
    }

    @Override
    void readFirstField(Block block, int position, DoubleAndBlockPositionValueState state)
    {
        state.setFirst(firstType.getDouble(block, position));
    }

    @Override
    void writeFirstField(BlockBuilder out, DoubleAndBlockPositionValueState state)
    {
        firstType.writeDouble(out, state.getFirst());
    }
}
