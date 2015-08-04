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

public class LongEncoding
        implements TextColumnEncoding
{
    private static final Slice MIN_LONG = Slices.utf8Slice("-9223372036854775808");
    private final Type type;
    private final Slice nullSequence;
    private final StringBuilder buffer = new StringBuilder();

    public LongEncoding(Type type, Slice nullSequence)
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
                long value = type.getLong(block, position);
                buffer.setLength(0);
                buffer.append(value);
                for (int index = 0; index < buffer.length(); index++) {
                    output.writeByte(buffer.charAt(index));
                }
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(int depth, Block block, int position, SliceOutput output)
    {
        long value = type.getLong(block, position);
        buffer.setLength(0);
        buffer.append(value);
        for (int index = 0; index < buffer.length(); index++) {
            output.writeByte(buffer.charAt(index));
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
            if (length == 0 || nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, parseLong(slice, offset, length));
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        type.writeLong(builder, parseLong(slice, offset, length));
    }

    private static long parseLong(Slice slice, int start, int length)
    {
        if (slice.equals(start, length, MIN_LONG, 0, MIN_LONG.length())) {
            return Long.MIN_VALUE;
        }

        int limit = start + length;

        int sign;
        if (slice.getByte(start) == '-') {
            sign = -1;
            start++;
        }
        else {
            sign = 1;
        }

        long value = slice.getByte(start) - ((int) '0');
        start++;
        while (start < limit) {
            value = value * 10 + (slice.getByte(start) - ((int) '0'));
            start++;
        }

        return value * sign;
    }
}
