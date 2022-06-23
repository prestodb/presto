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
import com.facebook.presto.common.type.UnscaledDecimal128Arithmetic;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;

public class LongDecimalWithOverflowAndLongStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    private static final int SERIALIZED_SIZE = (Long.BYTES * 2) + UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;

    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        Slice decimal = state.getLongDecimal();
        if (decimal == null) {
            out.appendNull();
        }
        else {
            long count = state.getLong();
            long overflow = state.getOverflow();
            VARBINARY.writeSlice(out, Slices.wrappedLongArray(count, overflow, decimal.getLong(0), decimal.getLong(Long.BYTES)));
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (!block.isNull(index)) {
            Slice slice = VARBINARY.getSlice(block, index);
            if (slice.length() != SERIALIZED_SIZE) {
                throw new IllegalStateException("Unexpected serialized state size: " + slice.length());
            }

            long count = slice.getLong(0);
            long overflow = slice.getLong(Long.BYTES);
            Slice decimal = Slices.wrappedLongArray(slice.getLong(Long.BYTES * 2), slice.getLong(Long.BYTES * 3));

            state.setLong(count);
            state.setOverflow(overflow);
            state.setLongDecimal(decimal);
        }
    }
}
