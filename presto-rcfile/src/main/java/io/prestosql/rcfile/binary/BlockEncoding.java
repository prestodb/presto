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
package io.prestosql.rcfile.binary;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.rcfile.ColumnData;
import io.prestosql.rcfile.EncodeOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public abstract class BlockEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public BlockEncoding(Type type)
    {
        this.type = type;
    }

    private final DynamicSliceOutput buffer = new DynamicSliceOutput(0);

    @Override
    public final void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                encodeValue(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public final void encodeValueInto(Block block, int position, SliceOutput output)
    {
        buffer.reset();
        encodeValue(block, position, buffer);

        // structural types nested in structural types are length prefixed
        Slice slice = buffer.slice();
        output.writeInt(Integer.reverseBytes(slice.length()));
        output.writeBytes(slice);
    }

    protected abstract void encodeValue(Block block, int position, SliceOutput output);

    @Override
    public final Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();

        Slice slice = columnData.getSlice();
        BlockBuilder builder = type.createBlockBuilder(null, size);
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
