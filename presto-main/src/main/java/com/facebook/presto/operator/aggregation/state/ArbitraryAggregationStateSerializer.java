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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;

public class ArbitraryAggregationStateSerializer
        implements AccumulatorStateSerializer<ArbitraryAggregationState>
{
    private final Type arrayType;

    public ArbitraryAggregationStateSerializer(Type type)
    {
        this.arrayType = new ArrayType(type);
    }

    @Override
    public Type getSerializedType()
    {
        return arrayType;
    }

    @Override
    public void serialize(ArbitraryAggregationState state, BlockBuilder out)
    {
        Block block = state.getValue();
        if (block == null) {
            out.appendNull();
        }
        else {
            arrayType.writeObject(out, block);
        }
    }

    @Override
    public void deserialize(Block block, int index, ArbitraryAggregationState state)
    {
        state.setValue(block.getObject(0, Block.class));
    }
}
