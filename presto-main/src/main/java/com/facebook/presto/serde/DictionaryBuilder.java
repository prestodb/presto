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

import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.RandomAccessBlock;
import com.facebook.presto.spi.type.Type;
import it.unimi.dsi.fastutil.ints.Int2IntOpenCustomHashMap;
import it.unimi.dsi.fastutil.ints.IntHash.Strategy;

public class DictionaryBuilder
{
    private static final int CURRENT_VALUE_POSITION = 0xFF_FF_FF_FF;

    private final BlockBuilder blockBuilder;

    private final BlockBuilderHashStrategy hashStrategy;
    private final Int2IntOpenCustomHashMap positions;

    private int nextPosition;

    public DictionaryBuilder(Type type)
    {
        this.blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());

        this.hashStrategy = new BlockBuilderHashStrategy();
        this.positions = new Int2IntOpenCustomHashMap(1024, hashStrategy);
        this.positions.defaultReturnValue(-1);
    }

    public int size()
    {
        return positions.size();
    }

    public int putIfAbsent(BlockCursor value)
    {
        hashStrategy.setCurrentValue(value);
        int position = positions.get(CURRENT_VALUE_POSITION);
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

        value.appendTo(blockBuilder);
        positions.put(position, position);

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
            if (offset == CURRENT_VALUE_POSITION) {
                return hashCurrentRow();
            }
            else {
                return hashOffset(offset);
            }
        }

        private int hashCurrentRow()
        {
            return currentValue.calculateHashCode();
        }

        public int hashOffset(int offset)
        {
            return blockBuilder.hashCode(offset);
        }

        @Override
        public boolean equals(int leftPosition, int rightPosition)
        {
            // current row always equals itself
            if (leftPosition == CURRENT_VALUE_POSITION && rightPosition == CURRENT_VALUE_POSITION) {
                return true;
            }

            // current row == position
            if (leftPosition == CURRENT_VALUE_POSITION) {
                return offsetEqualsCurrentValue(rightPosition);
            }

            // position == current row
            if (rightPosition == CURRENT_VALUE_POSITION) {
                return offsetEqualsCurrentValue(leftPosition);
            }

            // position == position
            return offsetEqualsOffset(leftPosition, rightPosition);
        }

        public boolean offsetEqualsOffset(int leftOffset, int rightOffset)
        {
            return blockBuilder.equals(leftOffset, blockBuilder, rightOffset);
        }

        public boolean offsetEqualsCurrentValue(int offset)
        {
            return blockBuilder.equals(offset, currentValue);
        }
    }
}
