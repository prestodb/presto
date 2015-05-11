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
import com.facebook.presto.PagePartitionFunction;
import com.facebook.presto.execution.TaskId;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class PartitionedOutputBufferManager
        implements OutputBufferManager
{
    protected final Consumer<OutputBuffers> outputBufferTarget;
    protected final BiFunction<Integer, Integer, PagePartitionFunction> partitionFunctionGenerator;
    @GuardedBy("this")
    private final Set<TaskId> bufferIds = new LinkedHashSet<>();
    @GuardedBy("this")
    private boolean noMoreBufferIds;

    public PartitionedOutputBufferManager(
            Consumer<OutputBuffers> outputBufferTarget,
            BiFunction<Integer, Integer, PagePartitionFunction> partitionFunctionGenerator)
    {
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");
        this.partitionFunctionGenerator = requireNonNull(partitionFunctionGenerator, "partitionFunctionGenerator is null");
    }

    @Override
    public synchronized void addOutputBuffer(TaskId bufferId)
    {
        if (noMoreBufferIds) {
            // a stage can move to a final state (e.g., failed) while scheduling, so ignore
            // the new buffers
            return;
        }
        bufferIds.add(bufferId);
    }

    @Override
    public void noMoreOutputBuffers()
    {
        OutputBuffers outputBuffers;
        synchronized (this) {
            if (noMoreBufferIds) {
                // already created the buffers
                return;
            }
            noMoreBufferIds = true;

            ImmutableMap.Builder<TaskId, PagePartitionFunction> buffers = ImmutableMap.builder();
            int partition = 0;
            int partitionCount = bufferIds.size();
            for (TaskId bufferId : bufferIds) {
                buffers.put(bufferId, partitionFunctionGenerator.apply(partition, partitionCount));
                partition++;
            }
            outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS
                    .withBuffers(buffers.build())
                    .withNoMoreBufferIds();
        }

        outputBufferTarget.accept(outputBuffers);
    }
}
