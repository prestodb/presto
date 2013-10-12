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
package com.facebook.presto.serde;

import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.block.RandomAccessBlock;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import it.unimi.dsi.fastutil.ints.Int2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash.Strategy;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.facebook.presto.operator.HashStrategyUtils.valueEquals;
import static com.facebook.presto.operator.HashStrategyUtils.valueHashCode;
import static com.google.common.base.Preconditions.checkNotNull;

public class DictionaryBuilder
{
    private static final int CURRENT_VALUE_OFFSET = 0xFF_FF_FF_FF;

    private final Type type;

    private final BlockBuilder blockBuilder;

    private final BlockBuilderHashStrategy hashStrategy;
    private final Int2IntOpenCustomHashMap offsetToPosition;

    private int nextPosition;

    public DictionaryBuilder(Type type)
    {
        this.type = checkNotNull(type, "type is null");

        this.blockBuilder = createBlockBuilder(new TupleInfo(type));

        this.hashStrategy = new BlockBuilderHashStrategy();
        this.offsetToPosition = new Int2IntOpenCustomHashMap(1024, hashStrategy);
        this.offsetToPosition.defaultReturnValue(-1);
    }

    public int size()
    {
        return offsetToPosition.size();
    }

    public int putIfAbsent(BlockCursor value)
    {
        hashStrategy.setCurrentValue(value);
        int position = offsetToPosition.get(CURRENT_VALUE_OFFSET);
        if (position < 0) {
            position = addNewValue(value);
        }
        return position;
    }

    public RandomAccessBlock build()
    {
        return blockBuilder.build().toRandomAccessBlock();
    }

    private int addNewValue(BlockCursor value)
    {
        int position = nextPosition++;

        int offset = blockBuilder.size();
        value.appendTupleTo(blockBuilder);
        offsetToPosition.put(offset, position);

        return position;
    }

    private class BlockBuilderHashStrategy
            implements Strategy
    {
        private BlockCursor currentValue;

        public void setCurrentValue(BlockCursor currentValue)
        {
            this.currentValue = currentValue;
        }

        @Override
        public int hashCode(int offset)
        {
            if (offset == CURRENT_VALUE_OFFSET) {
                return hashCurrentRow();
            }
            else {
                return hashOffset(offset);
            }
        }

        private int hashCurrentRow()
        {
            return valueHashCode(type, currentValue.getRawSlice(), currentValue.getRawOffset());
        }

        public int hashOffset(int offset)
        {
            return valueHashCode(type, blockBuilder.getRawSlice(), offset);
        }

        @Override
        public boolean equals(int leftPosition, int rightPosition)
        {
            // current row always equals itself
            if (leftPosition == CURRENT_VALUE_OFFSET && rightPosition == CURRENT_VALUE_OFFSET) {
                return true;
            }

            // current row == position
            if (leftPosition == CURRENT_VALUE_OFFSET) {
                return offsetEqualsCurrentValue(rightPosition);
            }

            // position == current row
            if (rightPosition == CURRENT_VALUE_OFFSET) {
                return offsetEqualsCurrentValue(leftPosition);
            }

            // position == position
            return offsetEqualsOffset(leftPosition, rightPosition);
        }

        public boolean offsetEqualsOffset(int leftOffset, int rightOffset)
        {
            return valueEquals(type, blockBuilder.getRawSlice(), leftOffset, blockBuilder.getRawSlice(), rightOffset);
        }

        public boolean offsetEqualsCurrentValue(int offset)
        {
            return valueEquals(type, blockBuilder.getRawSlice(), offset, currentValue.getRawSlice(), currentValue.getRawOffset());
        }
    }
}
