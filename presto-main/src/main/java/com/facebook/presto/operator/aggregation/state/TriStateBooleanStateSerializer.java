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

import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static com.facebook.presto.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;

public class TriStateBooleanStateSerializer
        implements AccumulatorStateSerializer<TriStateBooleanState>
{
    @Override
    public Type getSerializedType()
    {
        return BOOLEAN;
    }

    @Override
    public void serialize(TriStateBooleanState state, BlockBuilder out)
    {
        if (state.getByte() == NULL_VALUE) {
            out.appendNull();
        }
        else {
            out.writeByte(state.getByte() == TRUE_VALUE ? 1 : 0).closeEntry();
        }
    }

    @Override
    public void deserialize(Block block, int index, TriStateBooleanState state)
    {
        state.setByte(BOOLEAN.getBoolean(block, index) ? TRUE_VALUE : FALSE_VALUE);
    }
}
