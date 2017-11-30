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
import com.facebook.presto.operator.aggregation.GroupedTypedHistogramSharedValues;
import com.facebook.presto.operator.aggregation.TypedHistogram;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static it.unimi.dsi.fastutil.HashCommon.arraySize;

public class GroupedHistogramState
        extends AbstractGroupedAccumulatorState
        implements HistogramState
{
    private static final float FILL_RATIO = 0.9f;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedHistogramState.class).instanceSize();
    private final ObjectBigArray<TypedHistogram> typedHistogramList = new ObjectBigArray<>();
    private final Type keyType;
    private final int expectedEntriesCount;
    private long size;
    private BlockBuilder valuesBlockBuilder;

    public GroupedHistogramState(Type keyType, int expectedEntriesCount)
    {
        this.keyType = keyType;
        this.expectedEntriesCount = expectedEntriesCount;
    }

    @Override
    public void ensureCapacity(long size)
    {
        typedHistogramList.ensureCapacity(size);
    }

    @Override
    public TypedHistogram get()
    {
        long groupId = getGroupId();
        TypedHistogram typedHistogram = typedHistogramList.get(groupId);

        return typedHistogram == null ? null : typedHistogram.setGroupId(groupId);
    }

    @Override
    public void set(TypedHistogram value)
    {
        if (value instanceof GroupedTypedHistogramSharedValues) {
            typedHistogramList.set(getGroupId(), value);
        }
        else {
            slowPathSet(value);
        }
    }

    @Override
    public void deserialize(Block block, Type type, int expectedSize)
    {
        typedHistogramList.set(getGroupId(), new GroupedTypedHistogramSharedValues(block, type, expectedSize, getValuesBlockBuilder(type, expectedSize)));
    }

    @Override
    public void addMemoryUsage(long memory)
    {
        size += memory;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + size + typedHistogramList.sizeOf();
    }

    public static int computeBucketCount(int expectedSize, float fillRatio)
    {
        return arraySize(expectedSize, fillRatio);
    }

    private BlockBuilder getValuesBlockBuilder(Type type, int expectedSize)
    {
        if (valuesBlockBuilder == null) {
            valuesBlockBuilder = type.createBlockBuilder(null, computeBucketCount(expectedSize));
        }

        return valuesBlockBuilder;
    }

    // used in combine when the LHS state is not settable to he RHS
    private void slowPathSet(TypedHistogram value)
    {
        TypedHistogram typedHistogram = new GroupedTypedHistogramSharedValues(value.getType(), value.getExpectedSize(), valuesBlockBuilder);
        typedHistogram.setGroupId(getGroupId());
        typedHistogram.addAll(value);
        typedHistogramList.set(getGroupId(), typedHistogram);
    }

    private static int computeBucketCount(int expectedSize)
    {
        return computeBucketCount(expectedSize, FILL_RATIO);
    }
}
