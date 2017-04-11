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

import com.facebook.presto.rcfile.ColumnData;
import com.facebook.presto.rcfile.EncodeOutput;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;

public class ByteEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public ByteEncoding(Type type)
    {
        this.type = type;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                output.writeByte((byte) type.getLong(block, position));
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        output.writeByte((byte) type.getLong(block, position));
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int length = columnData.getLength(i);
            if (length != 0) {
                checkState(length == SIZE_OF_BYTE, "Bytes should be 1 byte");
                type.writeLong(builder, slice.getByte(columnData.getOffset(i)));
            }
            else {
                builder.appendNull();
            }
        }
        return builder.build();
    }

    @Override
    public int getValueOffset(Slice slice, int offset)
    {
        return 0;
    }

    @Override
    public int getValueLength(Slice slice, int offset)
    {
        return SIZE_OF_BYTE;
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        type.writeLong(builder, slice.getByte(offset));
    }
}
