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
package com.facebook.presto.resourcemanager;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;

import javax.annotation.security.RolesAllowed;
import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.core.UriInfo;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.server.security.RoleType.ADMIN;
import static com.facebook.presto.server.security.RoleType.USER;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/taskInfo")
@RolesAllowed({USER, ADMIN})
public class DistributedTaskInfoResource
{
    private final ResourceManagerClusterStateProvider clusterStateProvider;
    private final ResourceManagerProxy proxyHelper;

    @Inject
    public DistributedTaskInfoResource(
            ResourceManagerClusterStateProvider clusterStateProvider,
            ResourceManagerProxy proxyHelper)
    {
        this.clusterStateProvider = requireNonNull(clusterStateProvider, "clusterStateProvider is null");
        this.proxyHelper = requireNonNull(proxyHelper, "proxyHelper is null");
    }

    @GET
    @Path("{taskId}")
    public void getTaskInfo(@PathParam("taskId") TaskId taskId,
            @Context UriInfo uriInfo,
            @Context HttpServletRequest servletRequest,
            @Suspended AsyncResponse asyncResponse)
            throws WebApplicationException
    {
        proxyTaskInfoResponse(servletRequest, asyncResponse, uriInfo, taskId);
    }

    private URI createTaskInfoUri(BasicQueryInfo queryInfo, UriInfo uriInfo)
    {
        return UriBuilder.fromUri(queryInfo.getSelf()).replacePath(uriInfo.getPath()).build();
    }

    private void proxyTaskInfoResponse(HttpServletRequest servletRequest, AsyncResponse asyncResponse, UriInfo uriInfo, TaskId taskId)
    {
        QueryId queryId = taskId.getQueryId();
        Optional<BasicQueryInfo> queryInfo = clusterStateProvider.getClusterQueries().stream()
                .filter(query -> query.getQueryId().equals(queryId))
                .findFirst();

        if (queryInfo.isPresent()) {
            proxyHelper.performRequest(servletRequest, asyncResponse, createTaskInfoUri(queryInfo.get(), uriInfo));
        }
        else {
            asyncResponse.resume(Response.status(NOT_FOUND).type(MediaType.APPLICATION_JSON).build());
        }
    }
}
