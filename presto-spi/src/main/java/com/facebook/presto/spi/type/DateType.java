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

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

//
// A date is stored as days from 1970-01-01.
//
// Note: when dealing with a java.sql.Date it is important to remember that the value is stored
// as the number of milliseconds from 1970-01-01T00:00:00 in UTC but time must be midnight in
// the local time zone.  This mean when converting between a java.sql.Date and this
// type, the time zone offset must be added or removed to keep the time at midnight in UTC.
//
public final class DateType
        extends AbstractFixedWidthType
{
    public static final DateType DATE = new DateType();

    private DateType()
    {
        super(parseTypeSignature(StandardTypes.DATE), long.class, SIZE_OF_INT);
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

        int days = block.getInt(position, 0);
        return new SqlDate(days);
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
        int value = block.getInt(position, 0);
        return value;
    }

    @Override
    public void writeLong(BlockBuilder blockBuilder, long value)
    {
        blockBuilder.writeInt((int) value).closeEntry();
    }

    @Override
    public boolean equals(Object other)
    {
        return other == DATE;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
