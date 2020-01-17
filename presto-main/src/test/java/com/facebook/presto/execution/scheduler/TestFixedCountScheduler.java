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
import com.facebook.presto.metadata.InternalNode;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestFixedCountScheduler
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final MockRemoteTaskFactory taskFactory;

    public TestFixedCountScheduler()
    {
        taskFactory = new MockRemoteTaskFactory(executor, scheduledExecutor);
    }

    @AfterClass(alwaysRun = true)
    public void destroyExecutor()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdown();
    }

    @Test
    public void testSingleNode()
    {
        FixedCountScheduler nodeScheduler = new FixedCountScheduler(
                (node, partition) -> Optional.of(taskFactory.createTableScanTask(
                        new TaskId("test", 1, 0, 1),
                        node, ImmutableList.of(),
                        new PartitionedSplitCountTracker(delta -> {}))),
                generateRandomNodes(1));

        ScheduleResult result = nodeScheduler.schedule();
        assertTrue(result.isFinished());
        assertTrue(result.getBlocked().isDone());
        assertEquals(result.getNewTasks().size(), 1);
        assertTrue(result.getNewTasks().iterator().next().getNodeId().equals("other 0"));
    }

    @Test
    public void testMultipleNodes()
    {
        FixedCountScheduler nodeScheduler = new FixedCountScheduler(
                (node, partition) -> Optional.of(taskFactory.createTableScanTask(
                        new TaskId("test", 1, 0, 1),
                        node, ImmutableList.of(),
                        new PartitionedSplitCountTracker(delta -> {}))),
                generateRandomNodes(5));

        ScheduleResult result = nodeScheduler.schedule();
        assertTrue(result.isFinished());
        assertTrue(result.getBlocked().isDone());
        assertEquals(result.getNewTasks().size(), 5);
        assertEquals(result.getNewTasks().stream().map(RemoteTask::getNodeId).collect(toImmutableSet()).size(), 5);
    }

    private static List<InternalNode> generateRandomNodes(int count)
    {
        return IntStream.range(0, count)
                .mapToObj(i -> new InternalNode("other " + i, URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN, false))
                .collect(toImmutableList());
    }
}
