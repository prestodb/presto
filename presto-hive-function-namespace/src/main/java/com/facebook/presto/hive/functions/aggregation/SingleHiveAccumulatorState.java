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

import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.AggregationBuffer;
import org.openjdk.jol.info.ClassLayout;

import java.util.function.Supplier;

@SuppressWarnings("deprecation")
public final class SingleHiveAccumulatorState
        implements HiveAccumulatorState
{
    private static final long INSTANCE_SIZE = ClassLayout.parseClass(SingleHiveAccumulatorState.class).instanceSize();

    private transient AggregationBuffer buffer;
    private transient AggregationBuffer prevBuffer;
    private transient long prevBufferSize;

    public SingleHiveAccumulatorState(Supplier<AggregationBuffer> bufferSupplier)
    {
        this.buffer = bufferSupplier.get();
    }

    @Override
    public AggregationBuffer getAggregationBuffer()
    {
        return buffer;
    }

    @Override
    public void setAggregationBuffer(AggregationBuffer buffer)
    {
        this.buffer = buffer;
    }

    @Override
    public long getEstimatedSize()
    {
        if (buffer == null) {
            return INSTANCE_SIZE;
        }
        if (buffer != prevBuffer) {
            prevBufferSize = ClassLayout.parseClass(buffer.getClass()).instanceSize();
            prevBuffer = buffer;
        }
        return INSTANCE_SIZE + prevBufferSize;
    }
}
