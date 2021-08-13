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

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.Adder;
import com.facebook.presto.operator.aggregation.MapUnionSumResult;
import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.RealOperators;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeUtils.isExactNumericType;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class MapUnionSumStateFactory
        implements AccumulatorStateFactory<MapUnionSumState>
{
    private final Type keyType;
    private final Type valueType;
    private final Adder adder;

    public MapUnionSumStateFactory(Type keyType, Type valueType)
    {
        this.keyType = keyType;
        this.valueType = valueType;
        this.adder = getAdder(valueType);
    }

    @Override
    public MapUnionSumState createSingleState()
    {
        return new SingleState(keyType, valueType, adder);
    }

    @Override
    public Class<? extends MapUnionSumState> getSingleStateClass()
    {
        return SingleState.class;
    }

    @Override
    public MapUnionSumState createGroupedState()
    {
        return new GroupedState(keyType, valueType, adder);
    }

    @Override
    public Class<? extends MapUnionSumState> getGroupedStateClass()
    {
        return GroupedState.class;
    }

    public static class GroupedState
            extends AbstractGroupedAccumulatorState
            implements MapUnionSumState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedState.class).instanceSize();
        private final Type keyType;
        private final Type valueType;
        private final Adder adder;
        private final ObjectBigArray<MapUnionSumResult> pairs = new ObjectBigArray<>();
        private long size;

        public GroupedState(Type keyType, Type valueType, Adder adder)
        {
            this.keyType = keyType;
            this.valueType = valueType;
            this.adder = adder;
        }

        @Override
        public void ensureCapacity(long size)
        {
            pairs.ensureCapacity(size);
        }

        @Override
        public MapUnionSumResult get()
        {
            return pairs.get(getGroupId());
        }

        @Override
        public void set(MapUnionSumResult value)
        {
            requireNonNull(value, "value is null");

            MapUnionSumResult previous = get();
            if (previous != null) {
                size -= previous.getRetainedSizeInBytes();
            }

            pairs.set(getGroupId(), value);
            size += value.getRetainedSizeInBytes();
        }

        @Override
        public void addMemoryUsage(long memory)
        {
            size += memory;
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            return INSTANCE_SIZE + size + pairs.sizeOf();
        }

        @Override
        public Adder getAdder()
        {
            return adder;
        }
    }

    public static class SingleState
            implements MapUnionSumState
    {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleState.class).instanceSize();
        private final Type keyType;
        private final Type valueType;
        private final Adder adder;
        private MapUnionSumResult pair;

        public SingleState(Type keyType, Type valueType, Adder adder)
        {
            this.keyType = keyType;
            this.valueType = valueType;
            this.adder = adder;
        }

        @Override
        public MapUnionSumResult get()
        {
            return pair;
        }

        @Override
        public void set(MapUnionSumResult value)
        {
            pair = value;
        }

        @Override
        public void addMemoryUsage(long memory)
        {
        }

        @Override
        public Type getKeyType()
        {
            return keyType;
        }

        @Override
        public Type getValueType()
        {
            return valueType;
        }

        @Override
        public long getEstimatedSize()
        {
            long estimatedSize = INSTANCE_SIZE;
            if (pair != null) {
                estimatedSize += pair.getRetainedSizeInBytes();
            }
            return estimatedSize;
        }

        @Override
        public Adder getAdder()
        {
            return adder;
        }
    }

    private static final Adder LONG_ADDER = new Adder() {
        @Override
        public void writeSum(Type type, Block block1, int position1, Block block2, int position2, BlockBuilder blockBuilder)
        {
            type.writeLong(blockBuilder, BigintOperators.add(type.getLong(block1, position1), type.getLong(block2, position2)));
        }
    };

    private static final Adder DOUBLE_ADDER = new Adder() {
        @Override
        public void writeSum(Type type, Block block1, int position1, Block block2, int position2, BlockBuilder blockBuilder)
        {
            type.writeDouble(blockBuilder, DoubleOperators.add(type.getDouble(block1, position1), type.getDouble(block2, position2)));
        }
    };

    private static final Adder FLOAT_ADDER = new Adder() {
        @Override
        public void writeSum(Type type, Block block1, int position1, Block block2, int position2, BlockBuilder blockBuilder)
        {
            type.writeLong(blockBuilder, RealOperators.add(type.getLong(block1, position1), type.getLong(block2, position2)));
        }
    };

    private static Adder getAdder(Type type)
    {
        if (isExactNumericType(type)) {
            return LONG_ADDER;
        }

        if (DOUBLE.equals(type)) {
            return DOUBLE_ADDER;
        }

        if (REAL.equals(type)) {
            return FLOAT_ADDER;
        }

        checkState(false);
        return null;
    }
}
