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
package io.prestosql.operator.aggregation.state;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;

import static io.prestosql.operator.aggregation.state.TriStateBooleanState.FALSE_VALUE;
import static io.prestosql.operator.aggregation.state.TriStateBooleanState.NULL_VALUE;
import static io.prestosql.operator.aggregation.state.TriStateBooleanState.TRUE_VALUE;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;

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
