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

package com.facebook.presto.hive.functions.aggregation;

import com.facebook.presto.common.array.ObjectBigArray;
import com.facebook.presto.spi.function.GroupedAccumulatorState;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.Supplier;

@SuppressWarnings("deprecation")
public final class GroupedHiveAccumulatorState
        implements GroupedAccumulatorState, HiveAccumulatorState
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(GroupedHiveAccumulatorState.class).instanceSize();

    private final Supplier<AggregationBuffer> bufferSupplier;
    private ObjectBigArray<AggregationBuffer> buffers = new ObjectBigArray<>();
    private long groupId;

    public GroupedHiveAccumulatorState(Supplier<AggregationBuffer> bufferSupplier)
    {
        this.bufferSupplier = bufferSupplier;
    }

    @Override
    public void setGroupId(long groupId)
    {
        this.groupId = groupId;
    }

    @Override
    public void ensureCapacity(long size)
    {
        buffers.ensureCapacity(size);
    }

    @Override
    public AggregationBuffer getAggregationBuffer()
    {
        AggregationBuffer buffer = buffers.get(groupId);
        if (buffer == null) {
            buffers.set(groupId, bufferSupplier.get());
        }
        return buffers.get(groupId);
    }

    @Override
    public void setAggregationBuffer(AggregationBuffer buffer)
    {
        buffers.set(groupId, buffer);
    }

    @Override
    public long getEstimatedSize()
    {
        // TODO improve estimation
        return INSTANCE_SIZE + buffers.sizeOf();
    }
}
