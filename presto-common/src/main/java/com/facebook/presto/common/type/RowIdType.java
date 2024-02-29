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
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;

/**
 * TODO this is wrong. need a binary format
 * A row ID is a unique identifier for o row that is a colon separated string in
 * this format:
 * <p>
 * row_number:row_group_id:data_version:partition_id:table_id:table_guid.substring(0, 4)
 *
 * <ul>
 * <li>row_number: a 64 bit long, unique within the partition</li>
 * <li>row_group_id: the file name</li>
 * <li>data_version: version of the partition</li>
 * <li>partition_id: a 64 bit long</li>
 * <li>table_id: a 64 bit long</li>
 * </ul>
 * <p>
 * The Presto blocks holding this data encode them as slices; that is, UTF-8 strings.
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
    public Object getObjectValue(SqlFunctionProperties sqlFunctionProperties, Block block, int position)
    {
        if (position < 0) {
            throw new IndexOutOfBoundsException("Negative block position " + position);
        }
        if (block.getPositionCount() == 0 || block.isNull(position)) {
            return null;
        }
        return block.getSlice(position, 0, block.getSliceLength(position)).toStringUtf8();
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
        return true;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        String left = leftBlock.getSlice(leftPosition, 0, leftBlock.getSliceLength(leftPosition)).toStringUtf8();
        String right = rightBlock.getSlice(rightPosition, 0, rightBlock.getSliceLength(rightPosition)).toStringUtf8();

        String[] partsLeft = left.split("\\:");
        String[] partsRight = right.split("\\:");
        try {
            for (int i = partsLeft.length - 1; i > 0; i--) {
                if (!partsLeft[i].equals(partsRight[i])) {
                    return partsLeft[i].compareTo(partsRight[i]);
                }
            }
            Long rowNumberLeft = Long.valueOf(partsLeft[0]);
            Long rowNumberRight = Long.valueOf(partsRight[0]);
            return rowNumberLeft.compareTo(rowNumberRight);
        }
        catch (IndexOutOfBoundsException | NumberFormatException ex) {
            // fall back to lexigraphic string comparison
            return left.compareTo(right);
        }
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
        return XxHash64.hash(getSlice(block, position));
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        String left = leftBlock.getSlice(leftPosition, 0, leftBlock.getSliceLength(leftPosition)).toStringUtf8();
        String right = rightBlock.getSlice(rightPosition, 0, rightBlock.getSliceLength(rightPosition)).toStringUtf8();
        return left.equals(right);
    }
}
