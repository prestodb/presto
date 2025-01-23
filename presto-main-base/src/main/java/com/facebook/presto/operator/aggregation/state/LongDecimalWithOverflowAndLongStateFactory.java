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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.common.array.LongBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

public class LongDecimalWithOverflowAndLongStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowAndLongState>
{
    @Override
    public LongDecimalWithOverflowAndLongState createSingleState()
    {
        return new SingleLongDecimalWithOverflowAndLongState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowAndLongState> getSingleStateClass()
    {
        return SingleLongDecimalWithOverflowAndLongState.class;
    }

    @Override
    public LongDecimalWithOverflowAndLongState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowAndLongState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowAndLongState> getGroupedStateClass()
    {
        return GroupedLongDecimalWithOverflowAndLongState.class;
    }

    public static class GroupedLongDecimalWithOverflowAndLongState
            extends LongDecimalWithOverflowStateFactory.GroupedLongDecimalWithOverflowState
            implements LongDecimalWithOverflowAndLongState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedLongDecimalWithOverflowAndLongState.class).instanceSize();
        private final LongBigArray longs = new LongBigArray();

        @Override
        public void ensureCapacity(long size)
        {
            longs.ensureCapacity(size);
            super.ensureCapacity(size);
        }

        @Override
        public long getLong()
        {
            return longs.get(getGroupId());
        }

        @Override
        public void setLong(long value)
        {
            longs.set(getGroupId(), value);
        }

        @Override
        public void addLong(long value)
        {
            longs.add(getGroupId(), value);
        }

        @Override
        public void incrementLong()
        {
            longs.increment(getGroupId());
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + unscaledDecimals.sizeOf() + (numberOfElements * SingleLongDecimalWithOverflowAndLongState.SIZE) + (overflows == null ? 0 : overflows.sizeOf());
        }
    }

    public static class SingleLongDecimalWithOverflowAndLongState
            extends LongDecimalWithOverflowStateFactory.SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowAndLongState
    {
        public static final int SIZE = ClassLayout.parseClass(Slice.class).instanceSize() + UNSCALED_DECIMAL_128_SLICE_LENGTH + SIZE_OF_LONG * 2;

        protected long longValue;

        @Override
        public long getLong()
        {
            return longValue;
        }

        @Override
        public void setLong(long longValue)
        {
            this.longValue = longValue;
        }

        @Override
        public void addLong(long value)
        {
            this.longValue += value;
        }

        @Override
        public void incrementLong()
        {
            longValue++;
        }

        @Override
        public long getEstimatedSize()
        {
            if (getLongDecimal() == null) {
                return SIZE_OF_LONG;
            }
            return SIZE;
        }
    }
}
