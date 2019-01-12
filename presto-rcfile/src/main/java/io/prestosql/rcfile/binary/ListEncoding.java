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
package io.prestosql.rcfile.binary;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import static io.prestosql.rcfile.RcFileDecoderUtils.decodeVIntSize;
import static io.prestosql.rcfile.RcFileDecoderUtils.readVInt;
import static io.prestosql.rcfile.RcFileDecoderUtils.writeVInt;
import static java.lang.Math.toIntExact;

public class ListEncoding
        extends BlockEncoding
{
    private final BinaryColumnEncoding elementEncoding;

    public ListEncoding(Type type, BinaryColumnEncoding elementEncoding)
    {
        super(type);
        this.elementEncoding = elementEncoding;
    }

    @Override
    public void encodeValue(Block block, int position, SliceOutput output)
    {
        Block list = block.getObject(position, Block.class);
        writeVInt(output, list.getPositionCount());

        // write null bits
        int nullByte = 0;
        for (int elementIndex = 0; elementIndex < list.getPositionCount(); elementIndex++) {
            if (elementIndex != 0 && elementIndex % 8 == 0) {
                output.writeByte(nullByte);
                nullByte = 0;
            }
            if (!list.isNull(elementIndex)) {
                nullByte |= (1 << (elementIndex % 8));
            }
        }
        output.writeByte(nullByte);

        // write values
        for (int elementIndex = 0; elementIndex < list.getPositionCount(); elementIndex++) {
            if (!list.isNull(elementIndex)) {
                elementEncoding.encodeValueInto(list, elementIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        // entries in list
        int entries = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice.getByte(offset));

        // null bytes
        int nullByteCur = offset;
        int nullByteEnd = offset + (entries + 7) / 8;

        // read elements starting after null bytes
        int elementOffset = nullByteEnd;
        BlockBuilder arrayBuilder = builder.beginBlockEntry();
        for (int i = 0; i < entries; i++) {
            if ((slice.getByte(nullByteCur) & (1 << (i % 8))) != 0) {
                int valueOffset = elementEncoding.getValueOffset(slice, elementOffset);
                int valueLength = elementEncoding.getValueLength(slice, elementOffset);

                elementEncoding.decodeValueInto(arrayBuilder, slice, elementOffset + valueOffset, valueLength);

                elementOffset = elementOffset + valueOffset + valueLength;
            }
            else {
                arrayBuilder.appendNull();
            }
            // move onto the next null byte
            if (7 == (i % 8)) {
                nullByteCur++;
            }
        }
        builder.closeEntry();
    }
}
