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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.OutputBuffers.OutputBufferId;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.BufferType.PARTITIONED;
import static com.facebook.presto.OutputBuffers.createInitialEmptyOutputBuffers;
import static com.google.common.base.Preconditions.checkArgument;

@ThreadSafe
public class PartitionedOutputBufferManager
        implements OutputBufferManager
{
    private final Map<OutputBufferId, Integer> outputBuffers;

    public PartitionedOutputBufferManager(int partitionCount, Consumer<OutputBuffers> outputBufferTarget)
    {
        checkArgument(partitionCount >= 1, "partitionCount must be at least 1");

        ImmutableMap.Builder<OutputBufferId, Integer> partitions = ImmutableMap.builder();
        for (int partition = 0; partition < partitionCount; partition++) {
            partitions.put(new OutputBufferId(partition), partition);
        }

        OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(PARTITIONED)
                .withBuffers(partitions.build())
                .withNoMoreBufferIds();
        outputBufferTarget.accept(outputBuffers);

        this.outputBuffers = outputBuffers.getBuffers();
    }

    @Override
    public void addOutputBuffers(List<OutputBufferId> newBuffers, boolean noMoreBuffers)
    {
        // All buffers are created in the constructor, so just validate that this isn't
        // a request to add a new buffer
        for (OutputBufferId newBuffer : newBuffers) {
            Integer existingBufferId = outputBuffers.get(newBuffer);
            if (existingBufferId == null) {
                throw new IllegalStateException("Unexpected new output buffer " + newBuffer);
            }
            if (newBuffer.getId() != existingBufferId) {
                throw new IllegalStateException("newOutputBuffers has changed the assignment for task " + newBuffer);
            }
        }
    }
}
