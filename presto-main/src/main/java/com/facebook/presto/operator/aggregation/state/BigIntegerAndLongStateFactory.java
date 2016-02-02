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
import org.openjdk.jol.info.ClassLayout;

import java.math.BigInteger;

import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class BigIntegerAndLongStateFactory
        implements AccumulatorStateFactory<BigIntegerAndLongState>
{
    // This is an upper bound estimate
    // using 5 Integers to account for the sum part of an average aggregation being possibly more than 128-bit
    static final long BIG_INTEGER_APPROX_SIZE = ClassLayout.parseClass(BigInteger.class).instanceSize() + sizeOf(new int[0]) + 5 * SIZE_OF_INT;

    @Override
    public BigIntegerAndLongState createSingleState()
    {
        return new SingleBigIntegerAndLongState();
    }

    @Override
    public Class<? extends BigIntegerAndLongState> getSingleStateClass()
    {
        return SingleBigIntegerAndLongState.class;
    }

    @Override
    public BigIntegerAndLongState createGroupedState()
    {
        return new GroupedBigIntegerAndLongState();
    }

    @Override
    public Class<? extends BigIntegerAndLongState> getGroupedStateClass()
    {
        return GroupedBigIntegerAndLongState.class;
    }

    public static class GroupedBigIntegerAndLongState
            extends AbstractGroupedAccumulatorState
            implements BigIntegerAndLongState
    {
        private final ObjectBigArray<BigInteger> bigIntegers = new ObjectBigArray<>();
        private final LongBigArray longs = new LongBigArray();
        private long estimatedSizeOfBigIntegerObjects;

        @Override
        public void ensureCapacity(long size)
        {
            bigIntegers.ensureCapacity(size);
            longs.ensureCapacity(size);
        }

        @Override
        public BigInteger getBigInteger()
        {
            return bigIntegers.get(getGroupId());
        }

        @Override
        public void setBigInteger(BigInteger value)
        {
            requireNonNull(value, "value is null");
            if (getBigInteger() == null) {
                estimatedSizeOfBigIntegerObjects += BIG_INTEGER_APPROX_SIZE;
            }
            bigIntegers.set(getGroupId(), value);
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
        public long getEstimatedSize()
        {
            return bigIntegers.sizeOf() + longs.sizeOf() + estimatedSizeOfBigIntegerObjects + SIZE_OF_LONG;
        }
    }

    public static class SingleBigIntegerAndLongState
            implements BigIntegerAndLongState
    {
        private BigInteger bigInteger;
        private long longValue;

        @Override
        public BigInteger getBigInteger()
        {
            return bigInteger;
        }

        @Override
        public void setBigInteger(BigInteger bigInteger)
        {
            this.bigInteger = bigInteger;
        }

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
        public long getEstimatedSize()
        {
            if (bigInteger == null) {
                return SIZE_OF_LONG;
            }
            return BIG_INTEGER_APPROX_SIZE + SIZE_OF_LONG;
        }
    }
}
