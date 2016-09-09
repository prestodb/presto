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

import com.facebook.presto.client.NodeVersion;
import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.execution.NodeTaskMap.PartitionedSplitCountTracker;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFixedCountScheduler
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final MockRemoteTaskFactory taskFactory;

    public TestFixedCountScheduler()
    {
        taskFactory = new MockRemoteTaskFactory(executor);
    }

    @AfterClass
    public void destroyExecutor()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSingleNode()
            throws Exception
    {
        FixedCountScheduler nodeScheduler = new FixedCountScheduler(
                (node, partition) -> taskFactory.createTableScanTask(
                        new TaskId("test", 1, 1),
                        node, ImmutableList.of(),
                        new PartitionedSplitCountTracker(delta -> { })),
                generateRandomNodes(1));

        ScheduleResult result = nodeScheduler.schedule();
        assertTrue(result.isFinished());
        assertTrue(result.getBlocked().isDone());
        assertEquals(result.getNewTasks().size(), 1);
        assertTrue(result.getNewTasks().iterator().next().getNodeId().equals("other 0"));
    }

    @Test
    public void testMultipleNodes()
            throws Exception
    {
        FixedCountScheduler nodeScheduler = new FixedCountScheduler(
                (node, partition) -> taskFactory.createTableScanTask(
                        new TaskId("test", 1, 1),
                        node, ImmutableList.of(),
                        new PartitionedSplitCountTracker(delta -> { })),
                generateRandomNodes(5));

        ScheduleResult result = nodeScheduler.schedule();
        assertTrue(result.isFinished());
        assertTrue(result.getBlocked().isDone());
        assertEquals(result.getNewTasks().size(), 5);
        assertEquals(result.getNewTasks().stream().map(RemoteTask::getNodeId).collect(toImmutableSet()).size(), 5);
    }

    private static Map<Integer, Node> generateRandomNodes(int count)
    {
        ImmutableMap.Builder<Integer, Node> nodes = ImmutableMap.builder();
        for (int i = 0; i < count; i++) {
            nodes.put(i, new PrestoNode("other " + i, URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false));
        }
        return nodes.build();
    }
}
