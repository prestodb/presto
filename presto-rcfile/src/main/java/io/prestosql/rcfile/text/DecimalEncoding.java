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
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Decimals;
import io.prestosql.spi.type.Type;

import java.math.BigDecimal;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.prestosql.spi.type.Decimals.encodeUnscaledValue;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static java.math.RoundingMode.HALF_UP;

public class DecimalEncoding
        implements TextColumnEncoding
{
    private final DecimalType type;
    private final Slice nullSequence;
    private final char[] buffer = new char[100];

    public DecimalEncoding(Type type, Slice nullSequence)
    {
        this.type = (DecimalType) type;
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
        if (isShortDecimal(type)) {
            output.writeBytes(utf8Slice(Decimals.toString(type.getLong(block, position), type.getScale())));
        }
        else {
            output.writeBytes(utf8Slice(Decimals.toString(type.getSlice(block, position), type.getScale())));
        }
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
            if (length == 0 || nullSequence.equals(0, nullSequence.length(), slice, offset, length)) {
                builder.appendNull();
            }
            else if (isShortDecimal(type)) {
                type.writeLong(builder, parseLong(slice, offset, length));
            }
            else {
                type.writeSlice(builder, parseSlice(slice, offset, length));
            }
        }
        return builder.build();
    }

    @Override
    public void decodeValueInto(int depth, BlockBuilder builder, Slice slice, int offset, int length)
    {
        if (isShortDecimal(type)) {
            type.writeLong(builder, parseLong(slice, offset, length));
        }
        else {
            type.writeSlice(builder, parseSlice(slice, offset, length));
        }
    }

    private long parseLong(Slice slice, int offset, int length)
    {
        BigDecimal decimal = parseBigDecimal(slice, offset, length);
        return decimal.unscaledValue().longValue();
    }

    private Slice parseSlice(Slice slice, int offset, int length)
    {
        BigDecimal decimal = parseBigDecimal(slice, offset, length);
        return encodeUnscaledValue(decimal.unscaledValue());
    }

    private BigDecimal parseBigDecimal(Slice slice, int offset, int length)
    {
        checkArgument(length < buffer.length);
        for (int i = 0; i < length; i++) {
            buffer[i] = (char) slice.getByte(offset + i);
        }

        BigDecimal decimal = new BigDecimal(buffer, 0, length);

        checkState(decimal.scale() <= type.getScale(), "Read decimal value scale larger than column scale");
        decimal = decimal.setScale(type.getScale(), HALF_UP);
        checkState(decimal.precision() <= type.getPrecision(), "Read decimal precision larger than column precision");
        return decimal;
    }
}
