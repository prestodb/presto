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
import com.facebook.presto.execution.NodeTaskMap;
import com.facebook.presto.execution.PartialResultQueryManager;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.warnings.DefaultWarningCollector;
import com.facebook.presto.execution.warnings.WarningCollectorConfig;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.WarningCollector;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.execution.warnings.WarningHandlingLevel.NORMAL;
import static com.facebook.presto.spi.StandardWarningCode.PARTIAL_RESULT_WARNING;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

public class TestPartialResultQueryTaskTracker
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));
    private final ScheduledExecutorService scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("stageScheduledExecutor-%s"));
    private final PartialResultQueryManager partialResultQueryManager = new PartialResultQueryManager();
    private final WarningCollector warningCollector = new DefaultWarningCollector(new WarningCollectorConfig(), NORMAL);
    private final MockRemoteTaskFactory taskFactory;

    public TestPartialResultQueryTaskTracker()
    {
        taskFactory = new MockRemoteTaskFactory(executor, scheduledExecutor);
    }

    @AfterClass(alwaysRun = true)
    public void destroyExecutor()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdown();
        partialResultQueryManager.stop();
    }

    @Test
    public void testPartialResultQueryTaskTracker()
            throws Exception
    {
        PartialResultQueryTaskTracker tracker = new PartialResultQueryTaskTracker(partialResultQueryManager, 0.50, 2.0, warningCollector);
        InternalNode node1 = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.8"), new NodeVersion("1"), false, false);
        InternalNode node2 = new InternalNode(UUID.randomUUID().toString(), URI.create("https://192.0.2.9"), new NodeVersion("1"), false, false);
        TaskId taskId1 = new TaskId("test1", 1, 0, 1);
        TaskId taskId2 = new TaskId("test2", 2, 0, 1);
        RemoteTask task1 = taskFactory.createTableScanTask(taskId1, node1, ImmutableList.of(), new NodeTaskMap.NodeStatsTracker(delta -> {}, delta -> {}, (age, delta) -> {}));
        RemoteTask task2 = taskFactory.createTableScanTask(taskId2, node2, ImmutableList.of(), new NodeTaskMap.NodeStatsTracker(delta -> {}, delta -> {}, (age, delta) -> {}));

        tracker.trackTask(task1);
        tracker.trackTask(task2);

        // Assert that completion ratio is 0.0 since the tasks did not complete yet
        assertEquals(0.0, tracker.getTaskCompletionRatio());

        tracker.completeTaskScheduling();

        tracker.recordTaskFinish(task1.getTaskInfo());
        // Assert that completion ratio is 0.5 since we have set that task1 finished in above line
        assertEquals(0.5, tracker.getTaskCompletionRatio());

        // Assert that the query is added to query manager, queue size = 1 since the query reached minCompletion ratio of 0.5 and is eligible for partial results
        assertEquals(1, partialResultQueryManager.getQueueSize());

        // Sleep for 2 seconds so that we give enough time for query manager to cancel tasks and complete the query with partial results
        Thread.sleep(2000);
        assertEquals(0, partialResultQueryManager.getQueueSize());

        // Assert that partial result warning is set correctly
        assertEquals(1, warningCollector.getWarnings().size());
        PrestoWarning prestoWarning = warningCollector.getWarnings().get(0);

        // Assert that warning code is set to PARTIAL_RESULT_WARNING
        assertEquals(PARTIAL_RESULT_WARNING.toWarningCode(), prestoWarning.getWarningCode());

        // Assert that completion percent of 50.00 is specified correctly in the warning message
        assertEquals("Partial results are returned. Only 50.00 percent of the data is read.", prestoWarning.getMessage());
    }
}
