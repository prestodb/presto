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

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.OutputBuffers.BROADCAST_PARTITION_ID;
import static com.facebook.presto.OutputBuffers.INITIAL_EMPTY_OUTPUT_BUFFERS;
import static java.util.Objects.requireNonNull;

@ThreadSafe
class BroadcastOutputBufferManager
        implements OutputBufferManager
{
    private final Consumer<OutputBuffers> outputBufferTarget;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = INITIAL_EMPTY_OUTPUT_BUFFERS;

    public BroadcastOutputBufferManager(Consumer<OutputBuffers> outputBufferTarget)
    {
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");
    }

    @Override
    public void addOutputBuffers(List<OutputBuffer> newBuffers, boolean noMoreBuffers)
    {
        synchronized (this) {
            if (outputBuffers.isNoMoreBufferIds()) {
                // a stage can move to a final state (e.g., failed) while scheduling, so ignore
                // the new buffers
                return;
            }

            OutputBuffers originalOutputBuffers = outputBuffers;

            // Note: it does not matter which partition id the task is using, in broadcast all tasks read from the same partition
            for (OutputBuffer newBuffer : newBuffers) {
                outputBuffers = outputBuffers.withBuffer(newBuffer.getBufferId(), BROADCAST_PARTITION_ID);
            }

            if (noMoreBuffers) {
                outputBuffers = outputBuffers.withNoMoreBufferIds();
            }

            // don't update if nothing changed
            if (outputBuffers == originalOutputBuffers) {
                return;
            }
        }
        outputBufferTarget.accept(outputBuffers);
    }
}
