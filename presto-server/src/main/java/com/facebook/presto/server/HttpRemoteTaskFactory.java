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
package com.facebook.presto.server;

import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Node;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.Split;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.OutputReceiver;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final AsyncHttpClient httpClient;
    private final LocationFactory locationFactory;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final int maxConsecutiveErrorCount;
    private final Duration minErrorDuration;
    private final ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%d"));
    private final ThreadPoolExecutorMBean executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);

    @Inject
    public HttpRemoteTaskFactory(QueryManagerConfig config,
            @ForScheduler AsyncHttpClient httpClient,
            LocationFactory locationFactory,
            JsonCodec<TaskInfo> taskInfoCodec,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec)
    {
        this.httpClient = httpClient;
        this.locationFactory = locationFactory;
        this.taskInfoCodec = taskInfoCodec;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.maxConsecutiveErrorCount = config.getRemoteTaskMaxConsecutiveErrorCount();
        this.minErrorDuration = config.getRemoteTaskMinErrorDuration();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Override
    public RemoteTask createRemoteTask(Session session,
            TaskId taskId,
            Node node,
            PlanFragment fragment,
            Multimap<PlanNodeId, Split> initialSplits,
            Map<PlanNodeId, OutputReceiver> outputReceivers,
            Set<String> initialOutputIds)
    {
        return new HttpRemoteTask(session,
                taskId,
                node.getNodeIdentifier(),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                outputReceivers,
                initialOutputIds,
                httpClient,
                executor,
                maxConsecutiveErrorCount,
                minErrorDuration,
                taskInfoCodec,
                taskUpdateRequestCodec
        );
    }
}
