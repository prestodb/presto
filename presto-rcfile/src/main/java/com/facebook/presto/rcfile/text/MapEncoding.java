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
package com.facebook.presto.rcfile.text;

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.StandardErrorCode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.rcfile.RcFileCorruptionException;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

public class MapEncoding
        extends BlockEncoding
{
    private final TextColumnEncoding keyEncoding;
    private final TextColumnEncoding valueEncoding;

    public MapEncoding(Type type, Slice nullSequence,
            byte[] separators,
            Byte escapeByte,
            TextColumnEncoding keyEncoding,
            TextColumnEncoding valueEncoding)
    {
        super(type, nullSequence, separators, escapeByte);
        this.keyEncoding = keyEncoding;
        this.valueEncoding = valueEncoding;
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
            throws RcFileCorruptionException
    {
        byte elementSeparator = getSeparator(depth);
        byte keyValueSeparator = getSeparator(depth + 1);

        Block map = block.getBlock(position);
        boolean first = true;
        for (int elementIndex = 0; elementIndex < map.getPositionCount(); elementIndex += 2) {
            if (map.isNull(elementIndex)) {
                throw new PrestoException(StandardErrorCode.GENERIC_INTERNAL_ERROR, "Map must never contain null keys");
            }

            if (!first) {
                output.writeByte(elementSeparator);
            }
            first = false;
            keyEncoding.encodeValueInto(depth + 2, map, elementIndex, output);
            output.writeByte(keyValueSeparator);
            if (map.isNull(elementIndex + 1)) {
                output.writeBytes(nullSequence);
            }
            else {
                valueEncoding.encodeValueInto(depth + 2, map, elementIndex + 1, output);
            }
        }
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
            throws RcFileCorruptionException
    {
        byte elementSeparator = getSeparator(depth);
        byte keyValueSeparator = getSeparator(depth + 1);
        int end = offset + length;

        BlockBuilder mapBuilder = builder.beginBlockEntry();
        if (length > 0) {
            int elementOffset = offset;
            int keyValueSeparatorPosition = -1;
            while (offset < end) {
                byte currentByte = slice.getByte(offset);
                if (currentByte == elementSeparator) {
                    decodeEntryInto(depth, mapBuilder, slice, elementOffset, offset - elementOffset, keyValueSeparatorPosition);
                    elementOffset = offset + 1;
                    keyValueSeparatorPosition = -1;
                }
                else if (currentByte == keyValueSeparator && keyValueSeparatorPosition == -1) {
                    keyValueSeparatorPosition = offset;
                }
                else if (isEscapeByte(currentByte) && offset + 1 < length) {
                    // ignore the char after escape_char
                    offset++;
                }
                offset++;
            }
            decodeEntryInto(depth, mapBuilder, slice, elementOffset, offset - elementOffset, keyValueSeparatorPosition);
        }
        builder.closeEntry();
    }

    private void decodeEntryInto(int depth, BlockBuilder builder, Slice slice, int offset, int length, int keyValueSeparatorPosition)
            throws RcFileCorruptionException
    {
        // if there is no key value separator, the key is all the data and the value is null
        int keyLength;
        if (keyValueSeparatorPosition == -1) {
            keyLength = length;
        }
        else {
            keyLength = keyValueSeparatorPosition - offset;
        }

        // ignore null keys
        if (isNullSequence(slice, offset, keyLength)) {
            return;
        }

        // output the key
        keyEncoding.decodeValueInto(depth + 2, builder, slice, offset, keyLength);

        // output value
        if (keyValueSeparatorPosition == -1) {
            builder.appendNull();
        }
        else {
            int valueOffset = keyValueSeparatorPosition + 1;
            int valueLength = length - keyLength - 1;
            if (isNullSequence(slice, valueOffset, valueLength)) {
                builder.appendNull();
            }
            else {
                valueEncoding.decodeValueInto(depth + 2, builder, slice, valueOffset, valueLength);
            }
        }
    }
}
