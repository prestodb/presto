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

import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.facebook.presto.server.QueryStateInfo.createQueuedQueryStateInfo;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

@Path("/v1/queryState")
public class QueryStateInfoResource
{
    private final QueryManager queryManager;
    private final ResourceGroupManager<?> resourceGroupManager;

    @Inject
    public QueryStateInfoResource(
            QueryManager queryManager,
            ResourceGroupManager<?> resourceGroupManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public List<QueryStateInfo> getQueryStateInfos(@QueryParam("user") String user)
    {
        List<BasicQueryInfo> queryInfos = queryManager.getQueries();

        if (!isNullOrEmpty(user)) {
            queryInfos = queryInfos.stream()
                    .filter(queryInfo -> Pattern.matches(user, queryInfo.getSession().getUser()))
                    .collect(toImmutableList());
        }

        return queryInfos.stream()
                .filter(queryInfo -> !queryInfo.getState().isDone())
                .map(this::getQueryStateInfo)
                .collect(toImmutableList());
    }

    private QueryStateInfo getQueryStateInfo(BasicQueryInfo queryInfo)
    {
        Optional<ResourceGroupId> groupId = queryInfo.getResourceGroupId();
        if (queryInfo.getState() == QUEUED) {
            return createQueuedQueryStateInfo(
                    queryInfo,
                    groupId,
                    groupId.map(resourceGroupManager::getPathToRoot));
        }
        return createQueryStateInfo(queryInfo, groupId);
    }

    @GET
    @Path("{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryStateInfo getQueryStateInfo(@PathParam("queryId") String queryId)
            throws WebApplicationException
    {
        try {
            return getQueryStateInfo(queryManager.getQueryInfo(new QueryId(queryId)));
        }
        catch (NoSuchElementException e) {
            throw new WebApplicationException(NOT_FOUND);
        }
    }
}
