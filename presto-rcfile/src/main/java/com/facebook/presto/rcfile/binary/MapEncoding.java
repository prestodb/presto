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
import io.airlift.slice.Slice;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.decodeVIntSize;
import static com.facebook.presto.rcfile.RcFileDecoderUtils.readVInt;
import static java.lang.Math.toIntExact;

public class MapEncoding
        extends BlockEncoding
{
    private final BinaryColumnEncoding keyReader;
    private final BinaryColumnEncoding valueReader;

    public MapEncoding(Type type, BinaryColumnEncoding keyReader, BinaryColumnEncoding valueReader)
    {
        super(type);
        this.keyReader = keyReader;
        this.valueReader = valueReader;
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        // entries in list
        int entries = toIntExact(readVInt(slice, offset));
        offset += decodeVIntSize(slice.getByte(offset));

        // null bytes
        int nullByteCur = offset;
        int nullByteEnd = offset + (entries * 2 + 7) / 8;

        // read elements starting after null bytes
        int elementOffset = nullByteEnd;
        BlockBuilder mapBuilder = builder.beginBlockEntry();
        for (int i = 0; i < entries; i++) {
            // read key
            boolean nullKey;
            if ((slice.getByte(nullByteCur) & (1 << ((i * 2) % 8))) != 0) {
                int keyOffset = keyReader.getValueOffset(slice, elementOffset);
                int keyLength = keyReader.getValueLength(slice, elementOffset);

                keyReader.decodeValueInto(mapBuilder, slice, elementOffset + keyOffset, keyLength);
                nullKey = false;

                elementOffset = elementOffset + keyOffset + keyLength;
            }
            else {
                nullKey = true;
            }

            // ignore entries with a null key

            // read value
            if ((slice.getByte(nullByteCur) & (1 << ((i * 2 + 1) % 8))) != 0) {
                int valueOffset = valueReader.getValueOffset(slice, elementOffset);
                int valueLength = valueReader.getValueLength(slice, elementOffset);

                // ignore entries with a null key
                if (!nullKey) {
                    valueReader.decodeValueInto(mapBuilder, slice, elementOffset + valueOffset, valueLength);
                }

                elementOffset = elementOffset + valueOffset + valueLength;
            }
            else {
                // ignore entries with a null key
                if (!nullKey) {
                    mapBuilder.appendNull();
                }
            }

            // move onto the next null byte
            if (3 == (i % 4)) {
                nullByteCur++;
            }
        }
        builder.closeEntry();
    }
}
