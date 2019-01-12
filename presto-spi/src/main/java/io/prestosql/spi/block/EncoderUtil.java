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
package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.Optional;

final class EncoderUtil
{
    private EncoderUtil()
    {
    }

    /**
     * Append null values for the block as a stream of bits.
     */
    @SuppressWarnings({"NarrowingCompoundAssignment", "ImplicitNumericConversion"})
    public static void encodeNullsAsBits(SliceOutput sliceOutput, Block block)
    {
        boolean mayHaveNull = block.mayHaveNull();
        sliceOutput.writeBoolean(mayHaveNull);
        if (!mayHaveNull) {
            return;
        }

        int positionCount = block.getPositionCount();
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = 0;
            value |= block.isNull(position) ? 0b1000_0000 : 0;
            value |= block.isNull(position + 1) ? 0b0100_0000 : 0;
            value |= block.isNull(position + 2) ? 0b0010_0000 : 0;
            value |= block.isNull(position + 3) ? 0b0001_0000 : 0;
            value |= block.isNull(position + 4) ? 0b0000_1000 : 0;
            value |= block.isNull(position + 5) ? 0b0000_0100 : 0;
            value |= block.isNull(position + 6) ? 0b0000_0010 : 0;
            value |= block.isNull(position + 7) ? 0b0000_0001 : 0;
            sliceOutput.appendByte(value);
        }

        // write last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = 0;
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                value |= block.isNull(position) ? mask : 0;
                mask >>>= 1;
            }
            sliceOutput.appendByte(value);
        }
    }

    /**
     * Decode the bit stream created by encodeNullsAsBits.
     */
    public static Optional<boolean[]> decodeNullBits(SliceInput sliceInput, int positionCount)
    {
        if (!sliceInput.readBoolean()) {
            return Optional.empty();
        }

        // read null bits 8 at a time
        boolean[] valueIsNull = new boolean[positionCount];
        for (int position = 0; position < (positionCount & ~0b111); position += 8) {
            byte value = sliceInput.readByte();
            valueIsNull[position] = ((value & 0b1000_0000) != 0);
            valueIsNull[position + 1] = ((value & 0b0100_0000) != 0);
            valueIsNull[position + 2] = ((value & 0b0010_0000) != 0);
            valueIsNull[position + 3] = ((value & 0b0001_0000) != 0);
            valueIsNull[position + 4] = ((value & 0b0000_1000) != 0);
            valueIsNull[position + 5] = ((value & 0b0000_0100) != 0);
            valueIsNull[position + 6] = ((value & 0b0000_0010) != 0);
            valueIsNull[position + 7] = ((value & 0b0000_0001) != 0);
        }

        // read last null bits
        if ((positionCount & 0b111) > 0) {
            byte value = sliceInput.readByte();
            int mask = 0b1000_0000;
            for (int position = positionCount & ~0b111; position < positionCount; position++) {
                valueIsNull[position] = ((value & mask) != 0);
                mask >>>= 1;
            }
        }

        return Optional.of(valueIsNull);
    }
}
