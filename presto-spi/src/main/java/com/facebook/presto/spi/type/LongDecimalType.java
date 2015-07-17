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

package com.facebook.presto.spi.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.math.BigDecimal;
import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public final class LongDecimalType
        extends DecimalType
{
    public static final int MAX_PRECISION = 39;
    public static final int LONG_DECIMAL_LENGTH = 2 * SIZE_OF_LONG;

    protected LongDecimalType(int precision, int scale)
    {
        super(precision, scale, Slice.class, LONG_DECIMAL_LENGTH);
        validatePrecisionScale(precision, scale, MAX_PRECISION);
    }

    public static BigDecimal toBigDecimal(Slice valueSlice, int scale)
    {
        return new BigDecimal(new BigInteger(valueSlice.getBytes()), scale);
    }

    public static BigInteger unscaledValueToBigInteger(Slice valueSlice)
    {
        return new BigInteger(valueSlice.getBytes());
    }

    public static Slice unscaledValueToSlice(BigInteger unscaledValue)
    {
        Slice resultSlice = Slices.allocate(LONG_DECIMAL_LENGTH);
        byte[] bytes = unscaledValue.toByteArray();
        if (unscaledValue.signum() < 0) {
            // need to fill with 0xff for negative values as we
            // represent value in two's-complement representation.
            resultSlice.fill((byte) 0xff);
        }
        resultSlice.setBytes(LONG_DECIMAL_LENGTH - bytes.length, bytes);
        return resultSlice;
    }

    public static Slice unscaledValueToSlice(String unscaledValue)
    {
        return unscaledValueToSlice(new BigInteger(unscaledValue));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        Slice slice = block.getSlice(position, 0, LONG_DECIMAL_LENGTH);
        return new SqlDecimal(unscaledValueToBigInteger(slice), precision, scale);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, LONG_DECIMAL_LENGTH);
    }

    @Override
    public int hash(Block block, int position)
    {
        return block.hash(position, 0, LONG_DECIMAL_LENGTH);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        return leftBlock.compareTo(leftPosition, 0, LONG_DECIMAL_LENGTH, rightBlock, rightPosition, 0, LONG_DECIMAL_LENGTH);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, LONG_DECIMAL_LENGTH, blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    public void writeBigDecimal(BlockBuilder blockBuilder, BigDecimal value)
    {
        writeSlice(blockBuilder, bigDecimalToSlice(value));
    }

    public Slice bigDecimalToSlice(BigDecimal value)
    {
        return unscaledValueToSlice(value.unscaledValue());
    }
}
