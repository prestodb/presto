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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.AccumulatorState;

public class ReservoirSampleState
        implements AccumulatorState
{
    private ReservoirSample sample;
    private boolean isEmpty = true;

    public ReservoirSampleState(Type type)
    {
        sample = new ReservoirSample(type);
    }

    public ReservoirSampleState(ReservoirSampleState other)
    {
        sample = other.sample.clone();
    }

    public ReservoirSampleState(ReservoirSample sample)
    {
        this.sample = sample;
    }

    public void add(Block block, int position, long n)
    {
        isEmpty = false;
        if (!sample.isSampleInitialized()) {
            sample.initializeSample(n);
        }
        sample.add(block, position);
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
    }

    public ReservoirSample getSamples()
    {
        return sample;
    }

    public void serialize(BlockBuilder out)
    {
        sample.serialize(out);
    }

    public boolean isEmpty()
    {
        return isEmpty;
    }
}
