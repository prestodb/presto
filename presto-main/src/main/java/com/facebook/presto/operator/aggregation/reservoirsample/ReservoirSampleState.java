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

import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorState;

import static com.facebook.presto.common.type.BigintType.BIGINT;

public class ReservoirSampleState
        implements AccumulatorState
{
    private ReservoirSample sample;
    private final Type arrayType;

    private Block initialSample;
    private long initialSeenCount = -1;

    public ReservoirSampleState(Type type)
    {
        sample = new ReservoirSample(type);
        arrayType = new ArrayType(type);
        initialSample = new ArrayBlockBuilder(arrayType, null, 1).appendNull().build();
    }

    public ReservoirSampleState(ReservoirSampleState other)
    {
        sample = other.sample.clone();
        arrayType = other.arrayType;
        this.initialSample = other.initialSample;
        this.initialSeenCount = other.initialSeenCount;
    }

    public void initializeInitialSample(Block initialSample, long initialSeenCount)
    {
        if (this.initialSeenCount < 0) {
            this.initialSample = initialSample;
            this.initialSeenCount = initialSeenCount;
        }
    }

    public void add(Block block, int position, long n)
    {
        sample.add(block, position, n);
    }

    public void setSample(ReservoirSample sample)
    {
        this.sample = sample;
    }

    @Override
    public long getEstimatedSize()
    {
        return sample.estimatedInMemorySize();
    }

    public void mergeWith(ReservoirSampleState otherState)
    {
        sample.merge(otherState.sample);
        initializeInitialSample(otherState.getInitialSample(), otherState.getInitialSeenCount());
    }

    public ReservoirSample getSamples()
    {
        return sample;
    }

    public Block getInitialSample()
    {
        return initialSample;
    }

    public long getInitialSeenCount()
    {
        return initialSeenCount;
    }

    public void serialize(BlockBuilder out)
    {
        arrayType.appendTo(initialSample, 0, out);
        BIGINT.writeLong(out, initialSeenCount);
        sample.serialize(out);
    }
}
