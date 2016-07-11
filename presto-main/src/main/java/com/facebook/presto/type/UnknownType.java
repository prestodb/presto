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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.TypeSignature;

import static com.google.common.base.Preconditions.checkArgument;

public final class UnknownType
        extends AbstractFixedWidthType
{
    public static final UnknownType UNKNOWN = new UnknownType();
    public static final String NAME = "unknown";

    private UnknownType()
    {
        super(new TypeSignature(NAME), void.class, 0);
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
    public long hash(Block block, int position)
    {
        // Check that the position is valid
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return 0;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // Check that the position is valid
        checkArgument(leftBlock.isNull(leftPosition), "Expected NULL value for UnknownType");
        checkArgument(rightBlock.isNull(rightPosition), "Expected NULL value for UnknownType");
        return true;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        // Check that the position is valid
        checkArgument(leftBlock.isNull(leftPosition), "Expected NULL value for UnknownType");
        checkArgument(rightBlock.isNull(rightPosition), "Expected NULL value for UnknownType");
        return 0;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        // call is null in case position is out of bounds
        checkArgument(block.isNull(position), "Expected NULL value for UnknownType");
        return null;
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        blockBuilder.appendNull();
    }
}
