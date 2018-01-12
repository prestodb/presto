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
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class AreaUnderRocCurveStateSerializer
        implements AccumulatorStateSerializer<AreaUnderRocCurveState>
{
    private static final ArrayType SERIALIZED_TYPE = new ArrayType(new RowType(ImmutableList.of(BOOLEAN, DOUBLE), Optional.empty()));

    @Override
    public Type getSerializedType()
    {
        return SERIALIZED_TYPE;
    }

    @Override
    public void serialize(AreaUnderRocCurveState state, BlockBuilder out)
    {
        state.get().serialize(out);
    }

    @Override
    public void deserialize(Block block, int index, AreaUnderRocCurveState state)
    {
        state.deserialize(SERIALIZED_TYPE.getObject(block, index));
    }
}
