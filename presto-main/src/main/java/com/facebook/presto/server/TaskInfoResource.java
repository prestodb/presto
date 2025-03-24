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

import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.StageInfo;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.resourcemanager.ResourceManagerProxy;
import com.facebook.presto.spi.QueryId;
import com.google.inject.Inject;
import jakarta.annotation.security.RolesAllowed;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.UriBuilder;
import jakarta.ws.rs.core.UriInfo;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static jakarta.ws.rs.core.Response.Status.NOT_FOUND;
import static java.util.Objects.requireNonNull;

@Path("v1/taskInfo")
@RolesAllowed("ADMIN")
public class TaskInfoResource
{
    public static final String INCLUDE_LOCAL_QUERY_ONLY = "includeLocalQueryOnly";
    private final DispatchManager dispatchManager;
    private final QueryManager queryManager;
    private final boolean resourceManagerEnabled;
    private final InternalNodeManager internalNodeManager;
    private final Optional<ResourceManagerProxy> proxyHelper;

    @Inject
    public TaskInfoResource(
            DispatchManager dispatchManager,
            QueryManager queryManager,
            InternalNodeManager internalNodeManager,
            ServerConfig serverConfig,
            Optional<ResourceManagerProxy> proxyHelper)
    {
        this.dispatchManager = requireNonNull(dispatchManager, "dispatchManager is null");
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.internalNodeManager = requireNonNull(internalNodeManager, "internalNodeManager is null");
        this.resourceManagerEnabled = requireNonNull(serverConfig, "serverConfig is null").isResourceManagerEnabled();
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
    }

    @GET
    @Path("{taskId}")
    @Produces(MediaType.APPLICATION_JSON)
    public void getTaskInfo(@PathParam("taskId") TaskId taskId,
            @QueryParam(INCLUDE_LOCAL_QUERY_ONLY) @DefaultValue("false") boolean includeLocalQueryOnly,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
            throws WebApplicationException
    {
        if (requestNeedsToBeProxied(taskId, includeLocalQueryOnly)) {
            proxyTaskInfo(servletRequest, asyncResponse, uriInfo);
        }
        else {
            try {
                asyncResponse.resume(Response.ok(getTaskInfo(taskId)).build());
            }
            catch (NotFoundException e) {
                asyncResponse.resume(Response.status(NOT_FOUND).entity("Could not find the requested taskInfo").build());
            }
            catch (Exception e) {
                asyncResponse.resume(Response.serverError().entity(e.getMessage()).build());
            }
        }
    }

    private TaskInfo getTaskInfo(TaskId taskId)
    {
        QueryId queryId = taskId.getQueryId();
        try {
            Optional<StageInfo> stageInfo = queryManager.getFullQueryInfo(queryId).getOutputStage();

            if (stageInfo.isPresent()) {
                Optional<StageInfo> stage = stageInfo.get().getStageWithStageId(taskId.getStageExecutionId().getStageId());
                if (stage.isPresent()) {
                    Optional<TaskInfo> taskInfo = stage.get().getLatestAttemptExecutionInfo().getTasks().stream()
                            .filter(info -> info.getTaskId().equals(taskId))
                            .findFirst();

                    if (taskInfo.isPresent()) {
                        return taskInfo.get();
                    }
                }
            }
            throw new NotFoundException("TaskInfo not found for task id " + taskId.toString());
        }
        catch (Exception e) {
            throw new NotFoundException(e);
        }
    }

    private boolean requestNeedsToBeProxied(TaskId taskId, boolean includeLocalQueryOnly)
    {
        return !includeLocalQueryOnly
                && resourceManagerEnabled
                && !dispatchManager.isQueryPresent(taskId.getQueryId());
    }

    private URI createTaskInfoUri(UriInfo uriInfo, InternalNode resourceManagerNode)
            throws UnknownHostException
    {
        return UriBuilder.fromUri(uriInfo.getRequestUri())
                .queryParam(INCLUDE_LOCAL_QUERY_ONLY, true)
                .scheme(resourceManagerNode.getInternalUri().getScheme())
                .host(resourceManagerNode.getHostAndPort().toInetAddress().getHostName())
                .port(resourceManagerNode.getInternalUri().getPort())
                .build();
    }

    private void proxyTaskInfo(HttpServletRequest servletRequest, AsyncResponse asyncResponse, UriInfo uriInfo)
    {
        try {
            checkState(proxyHelper.isPresent());
            Optional<InternalNode> resourceManager = internalNodeManager.getResourceManagers().stream()
                    .findAny();
            if (resourceManager.isPresent()) {
                InternalNode resourceManagerNode = resourceManager.get();
                URI uri = createTaskInfoUri(uriInfo, resourceManagerNode);
                proxyHelper.get().performRequest(servletRequest, asyncResponse, uri);
            }
            else {
                asyncResponse.resume(Response.serverError().entity("Could not find the resource manager").build());
            }
        }
        catch (Exception e) {
            asyncResponse.resume(Response.serverError().entity(e.getMessage()).build());
        }
    }
}
