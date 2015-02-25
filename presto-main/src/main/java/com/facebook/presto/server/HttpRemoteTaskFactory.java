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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.execution.LocationFactory;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.RemoteTask;
import com.facebook.presto.execution.RemoteTaskFactory;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.ForScheduler;
import com.facebook.presto.spi.Node;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.collect.Multimap;
import io.airlift.concurrent.BoundedExecutor;
import io.airlift.concurrent.ExecutorServiceAdapter;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.http.client.HttpClient;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.inject.Inject;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class HttpRemoteTaskFactory
        implements RemoteTaskFactory
{
    private final HttpClient httpClient;
    private final LocationFactory locationFactory;
    private final JsonCodec<TaskInfo> taskInfoCodec;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final int maxConsecutiveErrorCount;
    private final Duration minErrorDuration;
    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;

    @Inject
    public HttpRemoteTaskFactory(QueryManagerConfig config,
            @ForScheduler HttpClient httpClient,
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
        ExecutorService coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = ExecutorServiceAdapter.from(new BoundedExecutor(coreExecutor, config.getRemoteTaskMaxCallbackThreads()));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) coreExecutor);
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
            OutputBuffers outputBuffers)
    {
        return new HttpRemoteTask(session,
                taskId,
                node.getNodeIdentifier(),
                locationFactory.createTaskLocation(node, taskId),
                fragment,
                initialSplits,
                outputBuffers,
                httpClient,
                executor,
                maxConsecutiveErrorCount,
                minErrorDuration,
                taskInfoCodec,
                taskUpdateRequestCodec
        );
    }
}
