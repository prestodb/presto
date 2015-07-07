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

import java.util.ArrayList;
import java.util.List;

import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;

public final class DecimalType
        extends AbstractFixedWidthType
{
    public static final int MAX_PRECISION = 19;
    private static final int UNSET = -1;

    public static DecimalType createDecimalType(int precision, int scale)
    {
        return new DecimalType(precision, scale);
    }

    public static DecimalType createDecimalType(int precision)
    {
        return new DecimalType(precision, 0);
    }

    public static DecimalType createUnparametrizedDecimal()
    {
        return new DecimalType();
    }

    private final int precision;
    private final int scale;

    private DecimalType()
    {
        super(new TypeSignature(StandardTypes.DECIMAL, emptyList(), emptyList()), long.class, SIZE_OF_LONG);
        this.precision = UNSET;
        this.scale = UNSET;
    }

    private DecimalType(int precision, int scale)
    {
        super(new TypeSignature(StandardTypes.DECIMAL, emptyList(), buildPrecisionScaleList(precision, scale)), long.class, SIZE_OF_LONG);

        validatePrecisionScale(precision, scale);

        this.precision = precision;
        this.scale = scale;
    }

    private void validatePrecisionScale(long precision, long scale)
    {
        if (precision < 0 || precision > MAX_PRECISION) {
            throw new IllegalArgumentException("Invalid DECIMAL precision " + precision);
        }

        if (scale < 0 || scale > precision) {
            throw new IllegalArgumentException("Invalid DECIMAL scale " + scale);
        }
    }

    private static List<Object> buildPrecisionScaleList(int precision, int scale)
    {
        List<Object> literalArguments = new ArrayList<>();
        literalArguments.add((long) precision);
        literalArguments.add((long) scale);
        return unmodifiableList(literalArguments);
    }

    public int getPrecision()
    {
        return precision;
    }

    public int getScale()
    {
        return scale;
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return true;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlDecimal(block.getLong(position, 0), precision, scale);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public int hash(Block block, int position)
    {
        return Long.hashCode(block.getLong(position, 0));
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        long leftValue = leftBlock.getLong(leftPosition, 0);
        long rightValue = rightBlock.getLong(rightPosition, 0);
        return Long.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeLong(block.getLong(position, 0)).closeEntry();
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return block.getLong(position, 0);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeLong(value).closeEntry();
    }
}
