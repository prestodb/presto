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

import java.util.List;

public class StructEncoding
        extends BlockEncoding
{
    private final boolean lastColumnTakesRest;
    private final List<TextColumnEncoding> structFields;

    public StructEncoding(Type type, Slice nullSequence,
            byte[] separators,
            Byte escapeByte,
            boolean lastColumnTakesRest,
            List<TextColumnEncoding> structFields)
    {
        super(type, nullSequence, separators, escapeByte);
        this.lastColumnTakesRest = lastColumnTakesRest;
        this.structFields = structFields;
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
            throws RcFileCorruptionException
    {
        byte separator = getSeparator(depth);

        Block row = block.getObject(position, Block.class);
        for (int fieldIndex = 0; fieldIndex < structFields.size(); fieldIndex++) {
            if (fieldIndex > 0) {
                output.writeByte(separator);
            }

            if (row.isNull(fieldIndex)) {
                output.writeBytes(nullSequence);
            }
            else {
                structFields.get(fieldIndex).encodeValueInto(depth + 1, row, fieldIndex, output);
            }
        }
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
            throws RcFileCorruptionException
    {
        byte separator = getSeparator(depth);
        int end = offset + length;

        BlockBuilder structBuilder = builder.beginBlockEntry();
        int elementOffset = offset;
        int fieldIndex = 0;
        while (offset < end && (!lastColumnTakesRest || fieldIndex < structFields.size())) {
            byte currentByte = slice.getByte(offset);
            if (currentByte == separator) {
                decodeElementValueInto(depth, fieldIndex, structBuilder, slice, elementOffset, offset - elementOffset);
                elementOffset = offset + 1;
                fieldIndex++;
            }
            else if (isEscapeByte(currentByte) && offset + 1 < length) {
                // ignore the char after escape_char
                offset++;
            }
            offset++;
        }
        decodeElementValueInto(depth, fieldIndex, structBuilder, slice, elementOffset, end - elementOffset);
        fieldIndex++;

        // missing fields are null
        while (fieldIndex < structFields.size()) {
            structBuilder.appendNull();
            fieldIndex++;
        }

        builder.closeEntry();
    }

    private void decodeElementValueInto(int depth, int fieldIndex, BlockBuilder builder, Slice slice, int offset, int length)
            throws RcFileCorruptionException
    {
        // ignore extra fields
        if (fieldIndex > structFields.size()) {
            return;
        }

        if (isNullSequence(slice, offset, length)) {
            builder.appendNull();
        }
        else {
            structFields.get(fieldIndex).decodeValueInto(depth + 1, builder, slice, offset, length);
        }
    }
}
