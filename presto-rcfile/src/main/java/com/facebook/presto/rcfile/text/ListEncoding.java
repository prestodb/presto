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

import com.facebook.presto.rcfile.RcFileCorruptionException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class ListEncoding
        extends BlockEncoding
{
    private final Slice nullSequence = Slices.utf8Slice("\\N");
    private final TextColumnEncoding elementReader;

    public ListEncoding(Type type, Slice nullSequence, byte[] separators, Byte escapeByte, TextColumnEncoding elementReader)
    {
        super(type, nullSequence, separators, escapeByte);
        this.elementReader = elementReader;
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
            elementReader.decodeValueInto(depth + 1, blockBuilder, slice, offset, length);
        }
    }
}
