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
package com.facebook.presto.execution.buffer;

import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.memory.context.LocalMemoryContext;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class TestingPartitionedOutputBuffer
        extends PartitionedOutputBuffer
{
    public TestingPartitionedOutputBuffer(
            String taskInstanceId,
            StateMachine<BufferState> state,
            OutputBuffers outputBuffers,
            DataSize maxBufferSize,
            Supplier<LocalMemoryContext> systemMemoryContextSupplier,
            Executor notificationExecutor)
    {
        super(taskInstanceId, state, outputBuffers, maxBufferSize, systemMemoryContextSupplier, notificationExecutor);
    }

    @Override
    public void enqueue(Lifespan lifespan, int partitionNumber, List<SerializedPage> pages)
    {
    }
}
