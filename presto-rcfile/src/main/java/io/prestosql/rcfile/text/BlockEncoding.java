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
import io.prestosql.rcfile.ColumnData;
import io.prestosql.rcfile.EncodeOutput;
import io.prestosql.rcfile.RcFileCorruptionException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

public abstract class BlockEncoding
        implements TextColumnEncoding
{
    private final Type type;
    protected final Slice nullSequence;
    private final byte[] separators;
    private final Byte escapeByte;

    public BlockEncoding(Type type, Slice nullSequence, byte[] separators, Byte escapeByte)
    {
        this.type = type;
        this.nullSequence = nullSequence;
        this.separators = separators;
        this.escapeByte = escapeByte;
    }

    @Override
    public final void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
            throws RcFileCorruptionException
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                encodeValueInto(1, block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public final Block decodeColumn(ColumnData columnData)
            throws RcFileCorruptionException
    {
        int size = columnData.rowCount();

        Slice slice = columnData.getSlice();
        BlockBuilder builder = type.createBlockBuilder(null, size);
        for (int i = 0; i < size; i++) {
            int length = columnData.getLength(i);
            int offset = columnData.getOffset(i);
            if (!isNullSequence(slice, offset, length)) {
                decodeValueInto(1, builder, slice, offset, length);
            }
            else {
                builder.appendNull();
            }
        }

        return builder.build();
    }

    protected final boolean isNullSequence(Slice slice, int offset, int length)
    {
        return nullSequence.equals(0, nullSequence.length(), slice, offset, length);
    }

    protected final boolean isEscapeByte(byte currentByte)
    {
        return escapeByte != null && currentByte == escapeByte;
    }

    protected final byte getSeparator(int depth)
    {
        return separators[depth];
    }
}
