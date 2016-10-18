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
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import static com.facebook.presto.rcfile.RcFileDecoderUtils.decodeVIntSize;
import static com.facebook.presto.rcfile.RcFileDecoderUtils.readVInt;

public class LongEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public LongEncoding(Type type)
    {
        this.type = type;
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
            if (length == 0) {
                builder.appendNull();
            }
            else {
                type.writeLong(builder, readVInt(slice, offset, length));
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
        return decodeVIntSize(slice, offset);
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        type.writeLong(builder, readVInt(slice, offset, length));
    }
}
