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

import com.facebook.presto.execution.MockRemoteTaskFactory;
import com.facebook.presto.metadata.NodeVersion;
import com.facebook.presto.metadata.PrestoNode;
import com.facebook.presto.spi.Node;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ExecutorService;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestCurrentNodeScheduler
{
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("stageExecutor-%s"));

    @AfterClass
    public void destroyExecutor()
    {
        executor.shutdownNow();
    }

    @Test
    public void test()
            throws Exception
    {
        MockRemoteTaskFactory taskFactory = new MockRemoteTaskFactory(executor);

        CurrentNodeScheduler nodeScheduler = new CurrentNodeScheduler(
                node -> taskFactory.createTableScanTask((Node) node, ImmutableList.of()),
                () -> new PrestoNode("other1", URI.create("http://127.0.0.1:11"), NodeVersion.UNKNOWN));

        ScheduleResult result = nodeScheduler.schedule();
        assertTrue(result.isFinished());
        assertTrue(result.getBlocked().isDone());
        assertEquals(result.getNewTasks().size(), 1);
        result.getNewTasks().iterator().next().getNodeId().equals("other1");
    }
}
