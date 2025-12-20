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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.type.TypeUtils.isExactNumericType;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public abstract class ArrayUnionSumResult
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(ArrayUnionSumResult.class).instanceSize();
    private static final int EXPECTED_ENTRY_SIZE = 16;

    protected final Type elementType;
    protected final Adder adder;

    public ArrayUnionSumResult(Type elementType, Adder adder)
    {
        this.elementType = requireNonNull(elementType, "elementType is null");
        this.adder = requireNonNull(adder, "adder is null");
    }

    abstract int size();
    abstract void appendValue(int i, BlockBuilder blockBuilder);
    abstract boolean isValueNull(int i);
    public abstract long getRetainedSizeInBytes();
    abstract Block getValueBlock();
    abstract int getValueBlockIndex(int i);

    public static ArrayUnionSumResult create(Type elementType, Adder adder, Block arrayBlock)
    {
        return new SingleArrayBlock(elementType, adder, arrayBlock);
    }

    public Type getElementType()
    {
        return elementType;
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        for (int i = 0; i < size(); i++) {
            appendValue(i, arrayBlockBuilder);
        }
        out.closeEntry();
    }

    static void appendValue(Type elementType, Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            if (isExactNumericType(elementType) || elementType instanceof RealType) {
                elementType.writeLong(blockBuilder, 0L);
            }
            else {
                elementType.writeDouble(blockBuilder, 0);
            }
        }
        else {
            elementType.appendTo(block, position, blockBuilder);
        }
    }

    /**
     * Add the values for corresponding indices from other to this.
     * The result array length is the maximum of the two input array lengths.
     * Missing elements are treated as 0, and null values are coalesced to 0.
     */
    public ArrayUnionSumResult unionSum(ArrayUnionSumResult other)
    {
        int thisSize = size();
        int otherSize = other.size();
        int resultSize = max(thisSize, otherSize);

        BlockBuilder resultValueBlockBuilder = elementType.createBlockBuilder(null, resultSize, expectedValueSize(elementType, EXPECTED_ENTRY_SIZE));

        for (int i = 0; i < resultSize; i++) {
            boolean thisHasValue = i < thisSize;
            boolean otherHasValue = i < otherSize;

            if (thisHasValue && otherHasValue) {
                boolean thisIsNull = isValueNull(i);
                boolean otherIsNull = other.isValueNull(i);

                if (!thisIsNull && !otherIsNull) {
                    // Both have non-null values, sum them
                    adder.writeSum(
                            elementType,
                            getValueBlock(),
                            getValueBlockIndex(i),
                            other.getValueBlock(),
                            other.getValueBlockIndex(i),
                            resultValueBlockBuilder);
                }
                else if (!thisIsNull) {
                    this.appendValue(i, resultValueBlockBuilder);
                }
                else {
                    other.appendValue(i, resultValueBlockBuilder);
                }
            }
            else if (thisHasValue) {
                this.appendValue(i, resultValueBlockBuilder);
            }
            else {
                other.appendValue(i, resultValueBlockBuilder);
            }
        }

        return new AccumulatedValues(elementType, adder, resultValueBlockBuilder.build());
    }

    public ArrayUnionSumResult unionSum(Block arrayBlock)
    {
        ArrayUnionSumResult arrayUnionSumResult = new SingleArrayBlock(elementType, adder, arrayBlock);
        return unionSum(arrayUnionSumResult);
    }

    /**
     * Holds the input array block to avoid unnecessary copy for the first array.
     */
    private static class SingleArrayBlock
            extends ArrayUnionSumResult
    {
        private final Block arrayBlock;

        public SingleArrayBlock(Type elementType, Adder adder, Block arrayBlock)
        {
            super(elementType, adder);
            this.arrayBlock = arrayBlock;
        }

        @Override
        int size()
        {
            return arrayBlock.getPositionCount();
        }

        @Override
        void appendValue(int i, BlockBuilder blockBuilder)
        {
            appendValue(elementType, arrayBlock, i, blockBuilder);
        }

        @Override
        boolean isValueNull(int i)
        {
            return arrayBlock.isNull(i);
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }

        @Override
        Block getValueBlock()
        {
            return arrayBlock;
        }

        @Override
        int getValueBlockIndex(int i)
        {
            return i;
        }
    }

    /**
     * Holds the result of aggregating two or more arrays.
     */
    private static class AccumulatedValues
            extends ArrayUnionSumResult
    {
        private final Block valueBlock;

        AccumulatedValues(Type elementType, Adder adder, Block valueBlock)
        {
            super(elementType, adder);
            this.valueBlock = valueBlock;
        }

        @Override
        int size()
        {
            return valueBlock.getPositionCount();
        }

        @Override
        void appendValue(int i, BlockBuilder blockBuilder)
        {
            appendValue(elementType, valueBlock, i, blockBuilder);
        }

        @Override
        boolean isValueNull(int i)
        {
            return valueBlock.isNull(i);
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + valueBlock.getRetainedSizeInBytes();
        }

        @Override
        Block getValueBlock()
        {
            return valueBlock;
        }

        @Override
        int getValueBlockIndex(int i)
        {
            return i;
        }
    }
}
