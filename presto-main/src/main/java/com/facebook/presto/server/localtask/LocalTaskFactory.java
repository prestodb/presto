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
package com.facebook.presto.server.localtask;

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.server.TaskUpdateRequest;
import com.facebook.presto.server.remotetask.RemoteTaskStats;
import com.facebook.presto.sql.planner.PlanFragment;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class LocalTaskFactory
{
    private final HttpClient httpClient;
    private final RemoteTaskStats stats;
    private final ExecutorService coreExecutor;
    private final Executor executor;
    private final ScheduledExecutorService updateScheduledExecutor;
    private final JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec;
    private final JsonCodec<PlanFragment> planFragmentCodec;
    private final JsonCodec<TaskInfo> taskInfoCodec;

    @Inject
    public LocalTaskFactory(
            @ForWorker HttpClient httpClient,
            RemoteTaskStats stats,
            JsonCodec<TaskUpdateRequest> taskUpdateRequestCodec,
            JsonCodec<PlanFragment> planFragmentCodec,
            JsonCodec<TaskInfo> taskInfoCodec)
    {
        this.httpClient = httpClient;
        this.stats = stats;
        this.taskUpdateRequestCodec = taskUpdateRequestCodec;
        this.planFragmentCodec = planFragmentCodec;
        this.taskInfoCodec = taskInfoCodec;

        this.coreExecutor = newCachedThreadPool(daemonThreadsNamed("remote-task-callback-%s"));
        this.executor = new BoundedExecutor(coreExecutor, 1000);
        this.updateScheduledExecutor = newSingleThreadScheduledExecutor(daemonThreadsNamed("task-info-update-scheduler-%s"));
    }

    public LocalTask createLocalTask(
            Session session,
            TaskId taskId,
            InternalNode node,
            PlanFragment fragment,
            List<TaskSource> taskSource,
            OutputBuffers outputBuffers,
            TableWriteInfo tableWriteInfo)
    {
        return new LocalTask(
                session,
                taskId,
                node.getNodeIdentifier(),
                // TODO figure out how to generate URI
                URI.create("http://127.0.0.1:7777/v1/task/" + taskId),
                fragment,
                taskSource,
                outputBuffers,
                httpClient,
                tableWriteInfo,
                stats,
                executor,
                updateScheduledExecutor,
                taskUpdateRequestCodec,
                planFragmentCodec,
                taskInfoCodec);
    }

    @PreDestroy
    public void stop()
    {
        coreExecutor.shutdownNow();
        updateScheduledExecutor.shutdownNow();
    }
}