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

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public abstract class BlockEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public BlockEncoding(Type type)
    {
        this.type = type;
    }

    @Override
    public final Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();

        Slice slice = columnData.getSlice();
        BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), size);
        for (int i = 0; i < size; i++) {
            int length = columnData.getLength(i);
            if (length > 0) {
                int offset = columnData.getOffset(i);
                decodeValueInto(builder, slice, offset, length);
            }
            else {
                builder.appendNull();
            }
        }

        return builder.build();
    }

    @Override
    public final int getValueLength(Slice slice, int offset)
    {
        return Integer.reverseBytes(slice.getInt(offset));
    }

    @Override
    public final int getValueOffset(Slice slice, int offset)
    {
        return SIZE_OF_INT;
    }
}
