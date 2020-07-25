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

import com.facebook.presto.common.block.AbstractArrayBlock;
import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.common.type.TypeUtils.hashPosition;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class ArrayType
        extends AbstractType
{
    private final Type elementType;
    public static final String ARRAY_NULL_ELEMENT_MSG = "ARRAY comparison not supported for arrays with null elements";

    public ArrayType(Type elementType)
    {
        super(new TypeSignature(ARRAY, TypeSignatureParameter.of(elementType.getTypeSignature())), Block.class);
        this.elementType = requireNonNull(elementType, "elementType is null");
    }

    public Type getElementType()
    {
        return elementType;
    }

    @Override
    public boolean isComparable()
    {
        return elementType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return elementType.isOrderable();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Block leftArray = leftBlock.getBlock(leftPosition);
        Block rightArray = rightBlock.getBlock(rightPosition);

        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            checkElementNotNull(leftArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            if (!elementType.equalTo(leftArray, i, rightArray, i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public long hash(Block block, int position)
    {
        Block array = getObject(block, position);
        long hash = 0;
        for (int i = 0; i < array.getPositionCount(); i++) {
            hash = 31 * hash + hashPosition(elementType, array, i);
        }
        return hash;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        if (!elementType.isOrderable()) {
            throw new UnsupportedOperationException(getTypeSignature() + " type is not orderable");
        }

        Block leftArray = leftBlock.getBlock(leftPosition);
        Block rightArray = rightBlock.getBlock(rightPosition);

        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        int index = 0;
        while (index < len) {
            checkElementNotNull(leftArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            int comparison = elementType.compareTo(leftArray, index, rightArray, index);
            if (comparison != 0) {
                return comparison;
            }
            index++;
        }

        if (index == len) {
            return leftArray.getPositionCount() - rightArray.getPositionCount();
        }

        return 0;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (block instanceof AbstractArrayBlock) {
            return ((AbstractArrayBlock) block).apply((valuesBlock, start, length) -> arrayBlockToObjectValues(properties, valuesBlock, start, length), position);
        }
        else {
            Block arrayBlock = block.getBlock(position);
            return arrayBlockToObjectValues(properties, arrayBlock, 0, arrayBlock.getPositionCount());
        }
    }

    private List<Object> arrayBlockToObjectValues(SqlFunctionProperties properties, Block block, int start, int length)
    {
        List<Object> values = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            values.add(elementType.getObjectValue(properties, block, i + start));
        }

        return Collections.unmodifiableList(values);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writePositionTo(position, blockBuilder);
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
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

    @Override
    public Block getObject(Block block, int position)
    {
        return block.getBlock(position);
    }

    @Override
    public Block getBlockUnchecked(Block block, int internalPosition)
    {
        return block.getBlockUnchecked(internalPosition);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        blockBuilder.appendStructure((Block) value);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new ArrayBlockBuilder(elementType, blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, 100);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return singletonList(getElementType());
    }

    @Override
    public String getDisplayName()
    {
        return ARRAY + "(" + elementType.getDisplayName() + ")";
    }
}
