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
package io.prestosql.rcfile.text;

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.rcfile.RcFileCorruptionException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

public class ListEncoding
        extends BlockEncoding
{
    private final TextColumnEncoding elementEncoding;

    public ListEncoding(Type type, Slice nullSequence, byte[] separators, Byte escapeByte, TextColumnEncoding elementEncoding)
    {
        super(type, nullSequence, separators, escapeByte);
        this.elementEncoding = elementEncoding;
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
            throws RcFileCorruptionException
    {
        byte separator = getSeparator(depth);

        Block list = block.getObject(position, Block.class);
        for (int elementIndex = 0; elementIndex < list.getPositionCount(); elementIndex++) {
            if (elementIndex > 0) {
                output.writeByte(separator);
            }
            if (list.isNull(elementIndex)) {
                output.writeBytes(nullSequence);
            }
            else {
                elementEncoding.encodeValueInto(depth + 1, list, elementIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
            throws RcFileCorruptionException
    {
        byte separator = getSeparator(depth);
        int end = offset + length;

        BlockBuilder arrayBlockBuilder = builder.beginBlockEntry();
        if (length > 0) {
            int elementOffset = offset;
            while (offset < end) {
                byte currentByte = slice.getByte(offset);
                if (currentByte == separator) {
                    decodeElementValueInto(depth, arrayBlockBuilder, slice, elementOffset, offset - elementOffset);
                    elementOffset = offset + 1;
                }
                else if (isEscapeByte(currentByte) && offset + 1 < length) {
                    // ignore the char after escape_char
                    offset++;
                }
                offset++;
            }
            decodeElementValueInto(depth, arrayBlockBuilder, slice, elementOffset, offset - elementOffset);
        }
        builder.closeEntry();
    }

    private void decodeElementValueInto(int depth, BlockBuilder blockBuilder, Slice slice, int offset, int length)
            throws RcFileCorruptionException
    {
        if (nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
            blockBuilder.appendNull();
        }
        else {
            elementEncoding.decodeValueInto(depth + 1, blockBuilder, slice, offset, length);
        }
    }
}
