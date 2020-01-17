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
import com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;

public class LongDecimalWithOverflowAndLongStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowAndLongState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(LongDecimalWithOverflowAndLongState state, BlockBuilder out)
    {
        if (state.getLongDecimal() == null) {
            out.appendNull();
        }
        else {
            Slice slice = Slices.allocate(Long.BYTES + Long.BYTES + UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH);
            SliceOutput output = slice.getOutput();

            output.writeLong(state.getLong());
            output.writeLong(state.getOverflow());
            output.writeBytes(state.getLongDecimal());

            VARBINARY.writeSlice(out, slice);
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowAndLongState state)
    {
        if (!block.isNull(index)) {
            SliceInput slice = VARBINARY.getSlice(block, index).getInput();

            state.setLong(slice.readLong());
            state.setOverflow(slice.readLong());
            state.setLongDecimal(Slices.copyOf(slice.readSlice(slice.available())));
        }
    }
}
