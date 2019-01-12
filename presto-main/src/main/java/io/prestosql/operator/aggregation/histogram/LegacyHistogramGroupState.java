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

package io.prestosql.operator.aggregation.histogram;

import io.prestosql.array.ObjectBigArray;
import io.prestosql.operator.aggregation.state.AbstractGroupedAccumulatorState;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

/**
 * original way of doing group-by: one histogram per group-by-id
 */
@Deprecated
public class LegacyHistogramGroupState
        extends AbstractGroupedAccumulatorState
        implements HistogramState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LegacyHistogramGroupState.class).instanceSize();
    private final ObjectBigArray<TypedHistogram> typedHistograms = new ObjectBigArray<>();
    private final Type keyType;
    private final int expectedEntriesCount;
    private long size;

    public LegacyHistogramGroupState(Type keyType, int expectedEntriesCount)
    {
        this.keyType = keyType;
        this.expectedEntriesCount = expectedEntriesCount;
    }

    @Override
    public void ensureCapacity(long size)
    {
        typedHistograms.ensureCapacity(size);
    }

    @Override
    public TypedHistogram get()
    {
        TypedHistogram typedHistogram = typedHistograms.get(getGroupId());

        if (typedHistogram == null) {
            SingleTypedHistogram newTypedHistogram = new SingleTypedHistogram(keyType, expectedEntriesCount);

            typedHistograms.set(getGroupId(), newTypedHistogram);
            typedHistogram = newTypedHistogram;
            size += typedHistogram.getEstimatedSize();
        }

        return typedHistogram;
    }

    @Override
    public void addMemoryUsage(long memory)
    {
        size += memory;
    }

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + size + typedHistograms.sizeOf();
    }

    @Override
    public void deserialize(Block block, Type type, int expectedSize)
    {
        typedHistograms.set(getGroupId(), new SingleTypedHistogram(block, type, expectedSize));
    }
}
