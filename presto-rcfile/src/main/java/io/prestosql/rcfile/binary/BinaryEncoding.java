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

public class BinaryEncoding
        implements BinaryColumnEncoding
{
    private final Type type;

    public BinaryEncoding(Type type)
    {
        this.type = type;
    }

    @Override
    public void encodeColumn(Block block, SliceOutput output, EncodeOutput encodeOutput)
    {
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                Slice slice = type.getSlice(block, position);
                if (slice.length() == 0) {
                    throw new IllegalArgumentException("RCBinary encoder does not support empty VARBINARY values (HIVE-2483). Use ORC or Parquet format instead.");
                }
                output.writeBytes(slice);
            }
            encodeOutput.closeEntry();
        }
    }

    @Override
    public void encodeValueInto(Block block, int position, SliceOutput output)
    {
        Slice slice = type.getSlice(block, position);
        // Note binary nested in complex structures do no use the empty marker.
        // Therefore, empty VARBINARY values are ok.
        writeVInt(output, slice.length());
        output.writeBytes(slice);
    }

    @Override
    public Block decodeColumn(ColumnData columnData)
    {
        int size = columnData.rowCount();
        BlockBuilder builder = type.createBlockBuilder(null, size);

        Slice slice = columnData.getSlice();
        for (int i = 0; i < size; i++) {
            int length = columnData.getLength(i);
            if (length > 0) {
                int offset = columnData.getOffset(i);
                type.writeSlice(builder, slice, offset, length);
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
        return decodeVIntSize(slice, offset);
    }

    @Override
    public int getValueLength(Slice slice, int offset)
    {
        return toIntExact(readVInt(slice, offset));
    }

    @Override
    public void decodeValueInto(BlockBuilder builder, Slice slice, int offset, int length)
    {
        type.writeSlice(builder, slice, offset, length);
    }
}
