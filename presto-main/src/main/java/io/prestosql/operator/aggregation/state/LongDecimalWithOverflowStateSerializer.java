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
package io.prestosql.operator.aggregation.state;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.AccumulatorStateSerializer;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.UnscaledDecimal128Arithmetic;

import static io.prestosql.spi.type.VarbinaryType.VARBINARY;

public class LongDecimalWithOverflowStateSerializer
        implements AccumulatorStateSerializer<LongDecimalWithOverflowState>
{
    @Override
    public Type getSerializedType()
    {
        return VARBINARY;
    }

    @Override
    public void serialize(LongDecimalWithOverflowState state, BlockBuilder out)
    {
        if (state.getLongDecimal() == null) {
            out.appendNull();
        }
        else {
            Slice slice = Slices.allocate(Long.BYTES + UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH);
            SliceOutput output = slice.getOutput();

            output.writeLong(state.getOverflow());
            output.writeBytes(state.getLongDecimal());

            VARBINARY.writeSlice(out, slice);
        }
    }

    @Override
    public void deserialize(Block block, int index, LongDecimalWithOverflowState state)
    {
        if (!block.isNull(index)) {
            SliceInput slice = VARBINARY.getSlice(block, index).getInput();

            state.setOverflow(slice.readLong());
            state.setLongDecimal(Slices.copyOf(slice.readSlice(slice.available())));
        }
    }
}
