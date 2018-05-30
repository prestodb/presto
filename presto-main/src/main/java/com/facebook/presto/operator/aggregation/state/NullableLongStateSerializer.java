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

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class NullableLongStateSerializer
        implements AccumulatorStateSerializer<NullableLongState>
{
    @Override
    public Type getSerializedType()
    {
        return BIGINT;
    }

    @Override
    public void serialize(NullableLongState state, BlockBuilder out)
    {
        if (state.isNull()) {
            out.appendNull();
        }
        else {
            BIGINT.writeLong(out, state.getLong());
        }
    }

    @Override
    public void deserialize(Block block, int index, NullableLongState state)
    {
        state.setNull(false);
        state.setLong(BIGINT.getLong(block, index));
    }
}
