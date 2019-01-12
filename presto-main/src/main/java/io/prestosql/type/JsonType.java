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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.AbstractVariableWidthType;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.spi.type.TypeSignature;

/**
 * The stack representation for JSON objects must have the keys in natural sorted order.
 */
public class JsonType
        extends AbstractVariableWidthType
{
    public static final JsonType JSON = new JsonType();

    public JsonType()
    {
        super(new TypeSignature(StandardTypes.JSON), Slice.class);
    }

    @Override
    public boolean isComparable()
    {
        return true;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Slice leftValue = leftBlock.getSlice(leftPosition, 0, leftBlock.getSliceLength(leftPosition));
        Slice rightValue = rightBlock.getSlice(rightPosition, 0, rightBlock.getSliceLength(rightPosition));
        return leftValue.equals(rightValue);
    }

    @Override
    public long hash(Block block, int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
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
            block.writeBytesTo(position, 0, block.getSliceLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
    }

    public void writeString(BlockBuilder blockBuilder, String value)
    {
        writeSlice(blockBuilder, Slices.utf8Slice(value));
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
}
