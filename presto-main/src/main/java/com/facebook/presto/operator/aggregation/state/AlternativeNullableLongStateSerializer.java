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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;

import static com.facebook.presto.common.type.BigintType.BIGINT;

/**
 * AlternativeNullableLongStateSerializer is only used for presto native worker.
 *
 * The only difference between AlternativeNullableLongStateSerializer and NullableLongStateSerializer
 * is the deserialize() function:
 * 1. NullableLongStateSerializer can only deserialize long type since presto java uses long as the unified
 * intermediate type for all integer data (long, int, smallint, tinyint).
 * 2. AlternativeNullableLongStateSerializer can deserialize any types that are compatible with long
 * (long, int, smallint, tinyint) since presto native use data's original types as intermediate types.
 */
public class AlternativeNullableLongStateSerializer
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
        // by using toLong(), we're able to deserialize compatible integer types with:
        // 1. no precision loss
        // 2. no performance difference
        state.setLong(block.toLong(index));
    }
}
