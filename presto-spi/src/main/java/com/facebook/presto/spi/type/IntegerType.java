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
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;

import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

public final class IntegerType
        extends AbstractFixedWidthType
{
    public static final IntegerType INTEGER = new IntegerType();

    private IntegerType()
    {
        super(parseTypeSignature(StandardTypes.INTEGER), long.class, SIZE_OF_INT);
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

        return block.getInt(position, 0);
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        int leftValue = leftBlock.getInt(leftPosition, 0);
        int rightValue = rightBlock.getInt(rightPosition, 0);
        return leftValue == rightValue;
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.getInt(position, 0);
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // WARNING: the correctness of InCodeGenerator is dependent on the implementation of this
        // function being the equivalence of internal long representation.
        int leftValue = leftBlock.getInt(leftPosition, 0);
        int rightValue = rightBlock.getInt(rightPosition, 0);
        return Integer.compare(leftValue, rightValue);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            blockBuilder.writeInt(block.getInt(position, 0)).closeEntry();
        }
    }

    @Override
    public long getLong(Block block, int position)
    {
        return (long) block.getInt(position, 0);
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        if (value > Integer.MAX_VALUE) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, String.format("Value %d exceeds MAX_INT", value));
        }
        else if (value < Integer.MIN_VALUE) {
            throw new PrestoException(NUMERIC_VALUE_OUT_OF_RANGE, String.format("Value %d is less than MIN_INT", value));
        }

        blockBuilder.writeInt((int) value).closeEntry();
    }

    @Override
    public boolean equals(Object other)
    {
        return other == INTEGER;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
