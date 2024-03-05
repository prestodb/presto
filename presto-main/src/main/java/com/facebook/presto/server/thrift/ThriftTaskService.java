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
package com.facebook.presto.server.thrift;

import com.facebook.drift.annotations.ThriftMethod;
import com.facebook.drift.annotations.ThriftService;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.execution.buffer.ThriftBufferResult;
import com.facebook.presto.server.ForAsyncRpc;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.presto.util.TaskUtils.DEFAULT_MAX_WAIT_TIME;
import static com.facebook.presto.util.TaskUtils.randomizeWaitTime;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.Objects.requireNonNull;

// TODO: the server currently only supports exchange; more end points (for /v1/task) should be supported
@ThriftService(value = "presto-task", idlName = "ThriftTaskService")
public class ThriftTaskService
{
    private final TaskManager taskManager;
    private final ScheduledExecutorService timeoutExecutor;

    @Inject
    public ThriftTaskService(TaskManager taskManager, @ForAsyncRpc ScheduledExecutorService timeoutExecutor)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
    }

    @ThriftMethod
    public ListenableFuture<ThriftBufferResult> getResults(TaskId taskId, OutputBufferId bufferId, long token, long maxSizeInBytes)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        ListenableFuture<BufferResult> bufferResultFuture = taskManager.getTaskResults(taskId, bufferId, token, new DataSize(maxSizeInBytes, BYTE));
        Duration waitTime = randomizeWaitTime(DEFAULT_MAX_WAIT_TIME);
        bufferResultFuture = addTimeout(
                bufferResultFuture,
                () -> BufferResult.emptyResults(taskManager.getTaskInstanceId(taskId), token, false),
                waitTime,
                timeoutExecutor);

        return Futures.transform(
                bufferResultFuture,
                ThriftBufferResult::fromBufferResult,
                directExecutor());
    }

    @ThriftMethod
    public ListenableFuture<Void> acknowledgeResults(TaskId taskId, OutputBufferId bufferId, long token)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.acknowledgeTaskResults(taskId, bufferId, token);
        return Futures.immediateFuture(null);
    }

    @ThriftMethod
    public ListenableFuture<Void> abortResults(TaskId taskId, OutputBufferId bufferId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        // Use thrift server pool to abort tasks; it is dangerous to use a fixed thread pool to abort tasks.
        // When having a surge of aborting results, a fixed thread pool may not be able to handle requests fast enough causing query to hang.
        // TaskManager does not support async calls with a thread pool.
        // Even getTaskResults is a fake async call with an immediate future wrapping around ClientBuffer::processRead.
        // It might worth exploring true async RPC for /v1/task endpoint
        taskManager.abortTaskResults(taskId, bufferId);
        return Futures.immediateFuture(null);
    }
}
