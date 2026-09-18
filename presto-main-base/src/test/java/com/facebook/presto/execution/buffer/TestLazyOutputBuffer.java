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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertFalse;

public class TestLazyOutputBuffer
{
    private static final String TASK_INSTANCE_ID = "task-instance-id";
    private static final OutputBufferId BUFFER_ID = new OutputBufferId(0);

    private ScheduledExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newScheduledThreadPool(2, daemonThreadsNamed("test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    private LazyOutputBuffer createLazyOutputBuffer()
    {
        return new LazyOutputBuffer(
                new TaskId("query", 0, 0, 0, 0),
                TASK_INSTANCE_ID,
                executor,
                1024L,
                () -> new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                new SpoolingOutputBufferFactory(new FeaturesConfig()));
    }

    // Before the delegate output buffer is created (setOutputBuffers has not run),
    // an acknowledge must be a no-op rather than throwing. A native worker's HEAD
    // get-data-size probe routes to acknowledge; failing hard turned it into a 500
    // and stalled the consumer. Mirrors the null-tolerant get()/abort() behavior.
    @Test
    public void testAcknowledgeBeforeInitializationIsNoOp()
    {
        LazyOutputBuffer buffer = createLazyOutputBuffer();
        buffer.acknowledge(BUFFER_ID, 0);
    }

    // Control: get() and abort() already tolerate a null delegate; verify they do
    // not throw either, so the acknowledge fix matches sibling behavior.
    @Test
    public void testGetAndAbortBeforeInitializationDoNotThrow()
    {
        LazyOutputBuffer buffer = createLazyOutputBuffer();
        assertFalse(buffer.get(BUFFER_ID, 0, 1024L).isDone());
        buffer.abort(BUFFER_ID);
    }
}
