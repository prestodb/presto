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
import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import static com.facebook.presto.common.type.UnscaledDecimal128Arithmetic.UNSCALED_DECIMAL_128_SLICE_LENGTH;
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
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedLongDecimalWithOverflowState.class).instanceSize();
        protected final ObjectBigArray<Slice> unscaledDecimals = new ObjectBigArray<>();
        @Nullable
        protected LongBigArray overflows;
        protected long numberOfElements;

        @Override
        public void ensureCapacity(long size)
        {
            unscaledDecimals.ensureCapacity(size);
            if (overflows != null) {
                overflows.ensureCapacity(size);
            }
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
            if (unscaledDecimals.getAndSet(getGroupId(), value) == null) {
                numberOfElements++;
            }
        }

        @Override
        public long getOverflow()
        {
            if (overflows == null) {
                return 0;
            }
            return overflows.get(getGroupId());
        }

        @Override
        public void setOverflow(long overflow)
        {
            // setOverflow(0) must overwrite any existing overflow value
            if (overflow == 0 && overflows == null) {
                return;
            }
            long groupId = getGroupId();
            if (overflows == null) {
                overflows = new LongBigArray();
                overflows.ensureCapacity(unscaledDecimals.getCapacity());
            }
            overflows.set(groupId, overflow);
        }

        @Override
        public void addOverflow(long overflow)
        {
            if (overflow == 0) {
                return;
            }
            long groupId = getGroupId();
            if (overflows == null) {
                overflows = new LongBigArray();
                overflows.ensureCapacity(unscaledDecimals.getCapacity());
            }
            overflows.add(groupId, overflow);
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + unscaledDecimals.sizeOf() + (numberOfElements * SingleLongDecimalWithOverflowState.SIZE) + (overflows == null ? 0 : overflows.sizeOf());
        }
    }

    public static class SingleLongDecimalWithOverflowState
            implements LongDecimalWithOverflowState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleLongDecimalWithOverflowState.class).instanceSize();
        public static final int SIZE = ClassLayout.parseClass(Slice.class).instanceSize() + UNSCALED_DECIMAL_128_SLICE_LENGTH;

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
        public void addOverflow(long overflow)
        {
            this.overflow += overflow;
        }

        @Override
        public long getEstimatedSize()
        {
            if (getLongDecimal() == null) {
                return INSTANCE_SIZE;
            }
            return INSTANCE_SIZE + SIZE;
        }
    }
}
