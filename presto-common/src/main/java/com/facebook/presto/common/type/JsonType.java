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
import com.facebook.presto.common.function.BlockIndex;
import com.facebook.presto.common.function.BlockPosition;
import com.facebook.presto.common.function.ScalarOperator;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.slice.XxHash64;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.common.type.TypeOperatorDeclaration.extractOperatorDeclaration;
import static java.lang.invoke.MethodHandles.lookup;

/**
 * The stack representation for JSON objects must have the keys in natural sorted order.
 */
public class JsonType
        extends AbstractVariableWidthType
{
    private static final TypeOperatorDeclaration TYPE_OPERATOR_DECLARATION = extractOperatorDeclaration(JsonType.class, lookup(), Slice.class);

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
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        return TYPE_OPERATOR_DECLARATION;
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
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
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

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(Slice left, Slice right)
    {
        return left.equals(right);
    }

    @ScalarOperator(EQUAL)
    private static boolean equalOperator(@BlockPosition Block leftBlock, @BlockIndex int leftPosition, @BlockPosition Block rightBlock, @BlockIndex int rightPosition)
    {
        int leftLength = leftBlock.getSliceLength(leftPosition);
        int rightLength = rightBlock.getSliceLength(rightPosition);
        if (leftLength != rightLength) {
            return false;
        }
        return leftBlock.equals(leftPosition, 0, rightBlock, rightPosition, 0, leftLength);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(Slice value)
    {
        return XxHash64.hash(value);
    }

    @ScalarOperator(XX_HASH_64)
    private static long xxHash64Operator(@BlockPosition Block block, @BlockIndex int position)
    {
        return block.hash(position, 0, block.getSliceLength(position));
    }
}
