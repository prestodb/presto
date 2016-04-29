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
package com.facebook.presto.rcfile.binary;

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.decodeVIntSize;
import static com.facebook.presto.rcfile.RcFileDecoderUtils.readVInt;

public class ListEncoding
        extends BlockEncoding
{
    private final BinaryColumnEncoding elementReader;

    public ListEncoding(Type type, BinaryColumnEncoding elementReader)
    {
        super(type);
        this.elementReader = elementReader;
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        // entries in list
        int entries = Ints.checkedCast(readVInt(slice, offset));
        offset += decodeVIntSize(slice.getByte(offset));

        // null bytes
        int nullByteCur = offset;
        int nullByteEnd = offset + (entries + 7) / 8;

        // read elements starting after null bytes
        int elementOffset = nullByteEnd;
        BlockBuilder arrayBuilder = builder.beginBlockEntry();
        for (int i = 0; i < entries; i++) {
            if ((slice.getByte(nullByteCur) & (1 << (i % 8))) != 0) {
                int valueOffset = elementReader.getValueOffset(slice, elementOffset);
                int valueLength = elementReader.getValueLength(slice, elementOffset);

                elementReader.decodeValueInto(arrayBuilder, slice, elementOffset + valueOffset, valueLength);

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
