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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

/**
 * A row ID is a unique identifier for a row that is a binary string. Typically, it has two
 * parts, one identifying the version of the partition and one identifying a specific row
 * within that partition.
 */
public class RowIdType
        extends AbstractVariableWidthType
{
    public static final RowIdType ROW_ID = new RowIdType();

    private RowIdType()
    {
        super(parseTypeSignature(StandardTypes.ROW_ID), Slice.class);
    }

    @Override
    public SqlVarbinary getObjectValue(SqlFunctionProperties sqlFunctionProperties, Block block, int position)
    {
        if (position < 0) {
            throw new IndexOutOfBoundsException("Negative block position " + position);
        }
        if (block.getPositionCount() == 0 || block.isNull(position)) {
            return null;
        }
        return new SqlVarbinary(block.getSlice(position, 0, block.getSliceLength(position)).getBytes());
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            int length = block.getSliceLength(position);
            blockBuilder.writeBytes(block.getSlice(position, 0, length), 0, length);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        if (value == null) {
            blockBuilder.appendNull();
        }
        else {
            writeSlice(blockBuilder, value, 0, value.length());
        }
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Slice left = leftBlock.getSlice(leftPosition, 0, leftBlock.getSliceLength(leftPosition));
        Slice right = rightBlock.getSlice(rightPosition, 0, rightBlock.getSliceLength(rightPosition));
        return left.equals(right);
    }
}
