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

import com.facebook.presto.rcfile.ColumnData;
import com.facebook.presto.rcfile.EncodeOutput;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

public class BooleanEncoding
        implements TextColumnEncoding
{
    private static final Slice TRUE = Slices.utf8Slice("TRUE");
    private static final Slice FALSE = Slices.utf8Slice("FALSE");

    private final Type type;
    private final Slice nullSequence;

    public BooleanEncoding(Type type, Slice nullSequence)
    {
        this.type = type;
        this.nullSequence = nullSequence;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                output.writeBytes(nullSequence);
            }
            else {
                encodeValue(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
    {
        encodeValue(block, position, output);
    }

    private void encodeValue(Block block, int position, SliceOutput output)
    {
        if (type.getBoolean(block, position)) {
            output.writeBytes(TRUE);
        }
        else {
            output.writeBytes(FALSE);
        }
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (isTrue(slice, offset, length)) {
                type.writeBoolean(builder, true);
            }
            else if (isFalse(slice, offset, length)) {
                type.writeBoolean(builder, false);
            }
            else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        type.writeBoolean(builder, isTrue(slice, offset, length));
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    private static boolean isFalse(Slice slice, int start, int length)
    {
        return (length == 5) &&
                (toUpperCase(slice.getByte(start + 0)) == 'F') &&
                (toUpperCase(slice.getByte(start + 1)) == 'A') &&
                (toUpperCase(slice.getByte(start + 2)) == 'L') &&
                (toUpperCase(slice.getByte(start + 3)) == 'S') &&
                (toUpperCase(slice.getByte(start + 4)) == 'E');
    }

    @SuppressWarnings("PointlessArithmeticExpression")
    private static boolean isTrue(Slice slice, int start, int length)
    {
        return (length == 4) &&
                (toUpperCase(slice.getByte(start + 0)) == 'T') &&
                (toUpperCase(slice.getByte(start + 1)) == 'R') &&
                (toUpperCase(slice.getByte(start + 2)) == 'U') &&
                (toUpperCase(slice.getByte(start + 3)) == 'E');
    }

    private static byte toUpperCase(byte b)
    {
        return isLowerCase(b) ? ((byte) (b - 32)) : b;
    }

    private static boolean isLowerCase(byte b)
    {
        return (b >= 'a') && (b <= 'z');
    }
}
