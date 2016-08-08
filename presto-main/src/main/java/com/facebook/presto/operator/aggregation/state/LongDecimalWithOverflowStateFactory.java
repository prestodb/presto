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

import com.facebook.presto.array.LongBigArray;
import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.Slice;

import static com.facebook.presto.spi.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.Objects.requireNonNull;

public class LongDecimalWithOverflowStateFactory
        implements AccumulatorStateFactory<LongDecimalWithOverflowState>
{
    @Override
    public LongDecimalWithOverflowState createSingleState()
    {
        return new SingleLongDecimalWithOverflowState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowState> getSingleStateClass()
    {
        return SingleLongDecimalWithOverflowState.class;
    }

    @Override
    public LongDecimalWithOverflowState createGroupedState()
    {
        return new GroupedLongDecimalWithOverflowState();
    }

    @Override
    public Class<? extends LongDecimalWithOverflowState> getGroupedStateClass()
    {
        return GroupedLongDecimalWithOverflowState.class;
    }

    public static class GroupedLongDecimalWithOverflowState
            extends AbstractGroupedAccumulatorState
            implements LongDecimalWithOverflowState
    {
        protected final ObjectBigArray<Slice> unscaledDecimals = new ObjectBigArray<>();
        protected final LongBigArray overflows = new LongBigArray();
        protected long numberOfElements;

        @Override
        public void ensureCapacity(long size)
        {
            unscaledDecimals.ensureCapacity(size);
            overflows.ensureCapacity(size);
        }

        @Override
        public Slice getLongDecimal()
        {
            return unscaledDecimals.get(getGroupId());
        }

        @Override
        public void setLongDecimal(Slice value)
        {
            requireNonNull(value, "value is null");
            if (getLongDecimal() == null) {
                numberOfElements++;
            }
            unscaledDecimals.set(getGroupId(), value);
        }

        @Override
        public long getOverflow()
        {
            return overflows.get(getGroupId());
        }

        @Override
        public void setOverflow(long overflow)
        {
            overflows.set(getGroupId(), overflow);
        }

        @Override
        public long getEstimatedSize()
        {
            return unscaledDecimals.sizeOf() + overflows.sizeOf() + numberOfElements * SingleLongDecimalWithOverflowState.SIZE;
        }
    }

    public static class SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowState
    {
        public static final int SIZE = SIZE_OF_LONG + UNSCALED_DECIMAL_128_SLICE_LENGTH;

        protected Slice unscaledDecimal;
        protected long overflow;

        @Override
        public Slice getLongDecimal()
        {
            return unscaledDecimal;
        }

        @Override
        public void setLongDecimal(Slice unscaledDecimal)
        {
            this.unscaledDecimal = unscaledDecimal;
        }

        @Override
        public long getOverflow()
        {
            return overflow;
        }

        @Override
        public void setOverflow(long overflow)
        {
            this.overflow = overflow;
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
