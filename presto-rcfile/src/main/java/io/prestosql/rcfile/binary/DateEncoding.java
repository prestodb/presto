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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.prestosql.rcfile.ColumnData;
import io.prestosql.rcfile.EncodeOutput;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import static io.prestosql.rcfile.RcFileDecoderUtils.decodeVIntSize;
import static io.prestosql.rcfile.RcFileDecoderUtils.readVInt;
import static io.prestosql.rcfile.RcFileDecoderUtils.writeVInt;
import static java.lang.Math.toIntExact;

public class DateEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public DateEncoding(Type type)
    {
        this.type = type;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                encodeValueInto(block, position, output);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        writeVInt(output, (int) type.getLong(block, position));
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int offset = columnData.getOffset(i);
            int length = columnData.getLength(i);
            if (length == 0) {
                builder.appendNull();
            }
            else {
                long daysSinceEpoch = readVInt(slice, offset, length);
                type.writeLong(builder, toIntExact(daysSinceEpoch));
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
        long daysSinceEpoch = readVInt(slice, offset, length);
        type.writeLong(builder, daysSinceEpoch);
    }
}
