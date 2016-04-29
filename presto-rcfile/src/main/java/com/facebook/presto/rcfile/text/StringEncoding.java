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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.calculateTruncationLength;

public class StringEncoding
        implements TextColumnEncoding
{
    private final Type type;
    private final Slice nullSequence;
    private final Byte escapeByte;

    public StringEncoding(Type type, Slice nullSequence, Byte escapeChar)
    {
        this.type = type;
        this.nullSequence = nullSequence;
        this.escapeByte = escapeChar;
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        if (escapeByte != null) {
            columnData = unescape(columnData, escapeByte);
        }

        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                length = calculateTruncationLength(type, slice, offset, length);
                type.writeSlice(builder, slice, offset, length);
            }
        }
        return builder.build();
    }

    @SuppressWarnings("AssignmentToForLoopParameter")
    private static ColumnData unescape(ColumnData columnData, byte escapeByte)
    {
        Slice slice = columnData.getSlice();
        // does slice contain escape byte
        if (slice.indexOfByte(escapeByte) < 0) {
            return columnData;
        }

        Slice newSlice = Slices.allocate(slice.length());
        SliceOutput output = newSlice.getOutput();
        int[] newOffsets = new int[columnData.rowCount() + 1];
        for (int row = 0; row < columnData.rowCount(); row++) {
            int offset = columnData.getOffset(row);
            int length = columnData.getLength(row);

            for (int i = 0; i < length; i++) {
                byte value = slice.getByte(offset + i);
                if (value == escapeByte && i + 1 < length) {
                    // read byte after escape
                    i++;
                    value = slice.getByte(offset + i);
                }
                output.write(value);
            }
            newOffsets[row + 1] = output.size();
        }
        return new ColumnData(newOffsets, output.slice());
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        length = calculateTruncationLength(type, slice, offset, length);
        type.writeSlice(builder, slice, offset, length);
    }
}
