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

import com.facebook.airlift.concurrent.BoundedExecutor;
import com.facebook.airlift.json.Codec;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorTypeSerdeManager;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers.OutputBufferId;
import com.facebook.presto.metadata.HandleResolver;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.HEAD;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.MoreFutures.addTimeout;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_BINARY;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_COMPACT;
import static com.facebook.airlift.http.client.thrift.ThriftRequestUtils.APPLICATION_THRIFT_FB_COMPACT;
import static com.facebook.airlift.http.server.AsyncResponseHandler.bindAsyncResponse;
import static com.facebook.presto.PrestoMediaTypes.APPLICATION_JACKSON_SMILE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_REMAINING_BYTES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CURRENT_STATE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_WAIT;
import static com.facebook.presto.server.TaskResourceUtils.convertToThriftTaskInfo;
import static com.facebook.presto.server.TaskResourceUtils.isThriftRequest;
import static com.facebook.presto.server.security.RoleType.INTERNAL;
import static com.facebook.presto.util.TaskUtils.randomizeWaitTime;
import static com.google.common.collect.Iterables.transform;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

/**
 * Manages tasks on this worker node
 */
@Path("/v1/task")
@RolesAllowed(INTERNAL)
public class TaskResource
{
    private static final Duration ADDITIONAL_WAIT_TIME = new Duration(5, SECONDS);

    private final TaskManager taskManager;
    private final SessionPropertyManager sessionPropertyManager;
    private final Executor responseExecutor;
    private final ScheduledExecutorService timeoutExecutor;
    private final Codec<PlanFragment> planFragmentCodec;
    private final HandleResolver handleResolver;
    private final ConnectorTypeSerdeManager connectorTypeSerdeManager;

    @Inject
    public TaskResource(
            TaskManager taskManager,
            SessionPropertyManager sessionPropertyManager,
            @ForAsyncRpc BoundedExecutor responseExecutor,
            @ForAsyncRpc ScheduledExecutorService timeoutExecutor,
            JsonCodec<PlanFragment> planFragmentJsonCodec,
            HandleResolver handleResolver,
            ConnectorTypeSerdeManager connectorTypeSerdeManager)
    {
        this.taskManager = requireNonNull(taskManager, "taskManager is null");
        this.sessionPropertyManager = requireNonNull(sessionPropertyManager, "sessionPropertyManager is null");
        this.responseExecutor = requireNonNull(responseExecutor, "responseExecutor is null");
        this.timeoutExecutor = requireNonNull(timeoutExecutor, "timeoutExecutor is null");
        this.planFragmentCodec = planFragmentJsonCodec;
        this.handleResolver = requireNonNull(handleResolver, "handleResolver is null");
        this.connectorTypeSerdeManager = requireNonNull(connectorTypeSerdeManager, "connectorTypeSerdeManager is null");
    }

    @GET
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public List<TaskInfo> getAllTaskInfo(@Context UriInfo uriInfo)
    {
        List<TaskInfo> allTaskInfo = taskManager.getAllTaskInfo();
        if (shouldSummarize(uriInfo)) {
            allTaskInfo = ImmutableList.copyOf(transform(allTaskInfo, TaskInfo::summarize));
        }
        return allTaskInfo;
    }

    @POST
    @Path("{taskId}")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public Response createOrUpdateTask(@PathParam("taskId") TaskId taskId, TaskUpdateRequest taskUpdateRequest, @Context UriInfo uriInfo)
    {
        requireNonNull(taskUpdateRequest, "taskUpdateRequest is null");

        Session session = taskUpdateRequest.getSession().toSession(sessionPropertyManager, taskUpdateRequest.getExtraCredentials());
        TaskInfo taskInfo = taskManager.updateTask(session,
                taskId,
                taskUpdateRequest.getFragment().map(planFragmentCodec::fromBytes),
                taskUpdateRequest.getSources(),
                taskUpdateRequest.getOutputIds(),
                taskUpdateRequest.getTableWriteInfo());

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }

        return Response.ok().entity(taskInfo).build();
    }

    @GET
    @Path("{taskId}")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public void getTaskInfo(
            @PathParam("taskId") final TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Context HttpHeaders httpHeaders,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        boolean isThriftRequest = isThriftRequest(httpHeaders);

        if (currentState == null || maxWait == null) {
            TaskInfo taskInfo = taskManager.getTaskInfo(taskId);
            if (shouldSummarize(uriInfo)) {
                taskInfo = taskInfo.summarize();
            }

            if (isThriftRequest) {
                taskInfo = convertToThriftTaskInfo(taskInfo, connectorTypeSerdeManager, handleResolver);
            }
            asyncResponse.resume(taskInfo);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        ListenableFuture<TaskInfo> futureTaskInfo = addTimeout(
                taskManager.getTaskInfo(taskId, currentState),
                () -> taskManager.getTaskInfo(taskId),
                waitTime,
                timeoutExecutor);

        if (shouldSummarize(uriInfo)) {
            futureTaskInfo = Futures.transform(futureTaskInfo, TaskInfo::summarize, directExecutor());
        }

        if (isThriftRequest) {
            futureTaskInfo = Futures.transform(
                    futureTaskInfo,
                    taskInfo -> convertToThriftTaskInfo(taskInfo, connectorTypeSerdeManager, handleResolver),
                    directExecutor());
        }

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskInfo, responseExecutor)
                .withTimeout(timeout);
    }

    @GET
    @Path("{taskId}/status")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public void getTaskStatus(
            @PathParam("taskId") TaskId taskId,
            @HeaderParam(PRESTO_CURRENT_STATE) TaskState currentState,
            @HeaderParam(PRESTO_MAX_WAIT) Duration maxWait,
            @Context UriInfo uriInfo,
            @Suspended AsyncResponse asyncResponse)
    {
        requireNonNull(taskId, "taskId is null");

        if (currentState == null || maxWait == null) {
            TaskStatus taskStatus = taskManager.getTaskStatus(taskId);
            asyncResponse.resume(taskStatus);
            return;
        }

        Duration waitTime = randomizeWaitTime(maxWait);
        // TODO: With current implementation, a newly completed driver group won't trigger immediate HTTP response,
        // leading to a slight delay of approx 1 second, which is not a major issue for any query that are heavy weight enough
        // to justify group-by-group execution. In order to fix this, REST endpoint /v1/{task}/status will need change.
        ListenableFuture<TaskStatus> futureTaskStatus = addTimeout(
                taskManager.getTaskStatus(taskId, currentState),
                () -> taskManager.getTaskStatus(taskId),
                waitTime,
                timeoutExecutor);

        // For hard timeout, add an additional time to max wait for thread scheduling contention and GC
        Duration timeout = new Duration(waitTime.toMillis() + ADDITIONAL_WAIT_TIME.toMillis(), MILLISECONDS);
        bindAsyncResponse(asyncResponse, futureTaskStatus, responseExecutor)
                .withTimeout(timeout);
    }

    @POST
    @Path("{taskId}/metadataresults")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE})
    public Response updateMetadataResults(@PathParam("taskId") TaskId taskId, MetadataUpdates metadataUpdates, @Context UriInfo uriInfo)
    {
        requireNonNull(metadataUpdates, "metadataUpdates is null");
        taskManager.updateMetadataResults(taskId, metadataUpdates);
        return Response.ok().build();
    }

    @DELETE
    @Path("{taskId}")
    @Consumes({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    @Produces({APPLICATION_JSON, APPLICATION_JACKSON_SMILE, APPLICATION_THRIFT_BINARY, APPLICATION_THRIFT_COMPACT, APPLICATION_THRIFT_FB_COMPACT})
    public TaskInfo deleteTask(
            @PathParam("taskId") TaskId taskId,
            @QueryParam("abort") @DefaultValue("true") boolean abort,
            @Context UriInfo uriInfo,
            @Context HttpHeaders httpHeaders)
    {
        requireNonNull(taskId, "taskId is null");
        TaskInfo taskInfo;

        if (abort) {
            taskInfo = taskManager.abortTask(taskId);
        }
        else {
            taskInfo = taskManager.cancelTask(taskId);
        }

        if (shouldSummarize(uriInfo)) {
            taskInfo = taskInfo.summarize();
        }

        if (isThriftRequest(httpHeaders)) {
            taskInfo = convertToThriftTaskInfo(taskInfo, connectorTypeSerdeManager, handleResolver);
        }

        return taskInfo;
    }

    @GET
    @Path("{taskId}/results/{bufferId}/{token}/acknowledge")
    public void acknowledgeResults(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.acknowledgeTaskResults(taskId, bufferId, token);
    }

    @HEAD
    @Path("{taskId}/results/{bufferId}")
    public Response taskResultsHeaders(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        OutputBufferInfo outputBufferInfo = taskManager.getOutputBufferInfo(taskId);
        return outputBufferInfo.getBuffers().stream()
                .filter(bufferInfo -> bufferInfo.getBufferId().equals(bufferId))
                .map(bufferInfo -> {
                    long bufferedBytes = bufferInfo.getPageBufferInfo().getBufferedBytes();
                    return Response.ok()
                            .header(PRESTO_BUFFER_REMAINING_BYTES, bufferedBytes)
                            .header(PRESTO_BUFFER_COMPLETE, bufferInfo.isFinished()
                                    // a buffer which has the noMorePages flag set and which is empty is completed
                                    || (!outputBufferInfo.getState().canAddPages() && bufferedBytes == 0))
                            .build();
                })
                .findFirst()
                .orElse(Response.status(Response.Status.NOT_FOUND).build());
    }

    @HEAD
    @Path("{taskId}/results/{bufferId}/{token}")
    public Response taskResultsHeaders(
            @PathParam("taskId") TaskId taskId,
            @PathParam("bufferId") OutputBufferId bufferId,
            @PathParam("token") final long token)
    {
        taskManager.acknowledgeTaskResults(taskId, bufferId, token);
        return taskResultsHeaders(taskId, bufferId);
    }

    @DELETE
    @Path("{taskId}/results/{bufferId}")
    @Produces(APPLICATION_JSON)
    public void abortResults(@PathParam("taskId") TaskId taskId, @PathParam("bufferId") OutputBufferId bufferId, @Context UriInfo uriInfo)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(bufferId, "bufferId is null");

        taskManager.abortTaskResults(taskId, bufferId);
    }

    @DELETE
    @Path("{taskId}/remote-source/{remoteSourceTaskId}")
    public void removeRemoteSource(@PathParam("taskId") TaskId taskId, @PathParam("remoteSourceTaskId") TaskId remoteSourceTaskId)
    {
        requireNonNull(taskId, "taskId is null");
        requireNonNull(remoteSourceTaskId, "remoteSourceTaskId is null");

        taskManager.removeRemoteSource(taskId, remoteSourceTaskId);
    }

    private static boolean shouldSummarize(UriInfo uriInfo)
    {
        return uriInfo.getQueryParameters().containsKey("summarize");
    }
}
