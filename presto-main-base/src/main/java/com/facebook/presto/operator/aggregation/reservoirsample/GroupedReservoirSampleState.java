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
package com.facebook.presto.operator.aggregation.reservoirsample;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.operator.aggregation.state.AbstractGroupedAccumulatorState;
import org.openjdk.jol.info.ClassLayout;

import static java.util.Objects.requireNonNull;

public class GroupedReservoirSampleState
        extends AbstractGroupedAccumulatorState
        implements ReservoirSampleState
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(GroupedReservoirSampleState.class).instanceSize();
    private final ObjectBigArray<ReservoirSample> samples = new ObjectBigArray<>();
    private long size;

    @Override
    public long getEstimatedSize()
    {
        return INSTANCE_SIZE + size + samples.sizeOf();
    }

    @Override
    public void ensureCapacity(long size)
    {
        samples.ensureCapacity(size);
    }

    @Override
    public ReservoirSample get()
    {
        return samples.get(getGroupId());
    }

    @Override
    public void set(ReservoirSample value)
    {
        requireNonNull(value, "value is null");
        ReservoirSample previous = get();
        if (previous != null) {
            size -= previous.estimatedInMemorySize();
        }

        samples.set(getGroupId(), value);
        size += value.estimatedInMemorySize();
    }
}
