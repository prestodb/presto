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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;

public class ApproximateMostFrequentStateSerializer
        implements AccumulatorStateSerializer<ApproximateMostFrequentState>
{
    private final Type type;

    public ApproximateMostFrequentStateSerializer(Type type)
    {
        this.type = type;
    }

    @Override
    public Type getSerializedType()
    {
        return new RowType(ImmutableList.of(BIGINT, new MapType(type, BIGINT)), Optional.empty());
    }

    @Override
    public void serialize(ApproximateMostFrequentState state, BlockBuilder out)
    {
        if (state.get() == null) {
            out.appendNull();
        }
        else {
            state.get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, ApproximateMostFrequentState state)
    {
        if (block.isNull(index)) {
            state.set(null);
        }
        else {
            Block subBlock = (Block) getSerializedType().getObject(block, index);
            state.set(TypedApproximateMostFrequentHistogram.deserialize(type, subBlock));
        }
    }
}
