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

import com.facebook.presto.operator.aggregation.MultiKeyValuePairs;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

public class MultiKeyValuePairStateSerializer
        implements AccumulatorStateSerializer<MultiKeyValuePairsState>
{
    private final ArrayType serializedType;

    public MultiKeyValuePairStateSerializer(Type keyType, Type valueType)
    {
        this.serializedType = new ArrayType(new RowType(ImmutableList.of(keyType, valueType), Optional.empty()));
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(MultiKeyValuePairsState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, MultiKeyValuePairsState state)
    {
        state.set(new MultiKeyValuePairs(serializedType.getObject(block, index), state.getKeyType(), state.getValueType()));
    }
}
