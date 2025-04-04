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

import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;

import javax.annotation.concurrent.GuardedBy;

import java.util.List;
import java.util.function.Consumer;

import static com.facebook.presto.execution.buffer.OutputBuffers.BufferType.ARBITRARY;
import static com.facebook.presto.execution.buffer.OutputBuffers.createInitialEmptyOutputBuffers;
import static java.util.Objects.requireNonNull;

public class ScaledOutputBufferManager
        implements OutputBufferManager
{
    private final Consumer<OutputBuffers> outputBufferTarget;

    @GuardedBy("this")
    private OutputBuffers outputBuffers = createInitialEmptyOutputBuffers(ARBITRARY);

    public ScaledOutputBufferManager(Consumer<OutputBuffers> outputBufferTarget)
    {
        this.outputBufferTarget = requireNonNull(outputBufferTarget, "outputBufferTarget is null");
        outputBufferTarget.accept(outputBuffers);
    }

    @SuppressWarnings("ObjectEquality")
    @Override
    public void addOutputBuffers(List<OutputBufferId> newBuffers, boolean noMoreBuffers)
    {
        OutputBuffers newOutputBuffers;
        synchronized (this) {
            if (outputBuffers.isNoMoreBufferIds()) {
                // a stage can move to a final state (e.g., failed) while scheduling,
                // so ignore the new buffers
                return;
            }

            OutputBuffers originalOutputBuffers = outputBuffers;

            for (OutputBufferId newBuffer : newBuffers) {
                outputBuffers = outputBuffers.withBuffer(newBuffer, newBuffer.getId());
            }

            if (noMoreBuffers) {
                outputBuffers = outputBuffers.withNoMoreBufferIds();
            }

            // don't update if nothing changed
            if (outputBuffers == originalOutputBuffers) {
                return;
            }
            newOutputBuffers = this.outputBuffers;
        }
        outputBufferTarget.accept(newOutputBuffers);
    }
}
