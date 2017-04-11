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

import com.facebook.presto.operator.aggregation.TypedHistogram;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.MapType;

import static com.facebook.presto.operator.aggregation.Histogram.EXPECTED_SIZE_FOR_HASHING;

public class HistogramStateSerializer
        implements AccumulatorStateSerializer<HistogramState>
{
    private final Type type;
    private final Type serializedType;

    public HistogramStateSerializer(Type type)
    {
        this.type = type;
        this.serializedType = new MapType(type, BigintType.BIGINT);
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(HistogramState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            serializedType.writeObject(out, state.get().serialize());
        }
    }

    @Override
    public void deserialize(Block block, int index, HistogramState state)
    {
        state.set(new TypedHistogram((Block) serializedType.getObject(block, index), type, EXPECTED_SIZE_FOR_HASHING));
    }
}
