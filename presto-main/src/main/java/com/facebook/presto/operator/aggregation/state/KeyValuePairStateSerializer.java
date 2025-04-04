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
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.KeyValuePairs;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

public class KeyValuePairStateSerializer
        implements AccumulatorStateSerializer<KeyValuePairsState>
{
    private final MapType mapType;

    public KeyValuePairStateSerializer(MapType mapType)
    {
        this.mapType = mapType;
    }

    @Override
    public Type getSerializedType()
    {
        return mapType;
    }

    @Override
    public void serialize(KeyValuePairsState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, KeyValuePairsState state)
    {
        state.set(new KeyValuePairs(mapType.getObject(block, index), state.getKeyType(), state.getValueType()));
    }
}
