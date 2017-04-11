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

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.resourceGroups.ResourceGroupManager;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import static com.facebook.presto.server.QueryStateInfo.createQueryStateInfo;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

@Path("/v1/queryState")
public class QueryStateInfoResource
{
    private final QueryManager queryManager;
    private final ResourceGroupManager resourceGroupManager;

    @Inject
    public QueryStateInfoResource(
            QueryManager queryManager,
            ResourceGroupManager resourceGroupManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        this.resourceGroupManager = requireNonNull(resourceGroupManager, "resourceGroupManager is null");
    }

    @GET
    public List<QueryStateInfo> getQueryDiagnosticsByUser(@QueryParam("user") String user)
    {
        if (isNullOrEmpty(user)) {
            return ImmutableList.of();
        }
        List<QueryInfo> userQueries = queryManager.getAllQueryInfo().stream()
                .filter(queryInfo -> Pattern.matches(user, queryInfo.getSession().getUser()))
                .filter(queryInfo -> !queryInfo.getState().isDone())
                .collect(toImmutableList());

        Map<ResourceGroupId, ResourceGroupInfo> rootResourceGroupInfos = userQueries.stream()
                .map(queryInfo -> queryManager.getQueryResourceGroup(queryInfo.getQueryId()))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(ResourceGroupId::getRoot)
                .distinct()
                .collect(toImmutableMap(identity(), resourceGroupManager::getResourceGroupInfo));

        return userQueries.stream()
                .map(queryInfo -> createQueryStateInfo(
                        queryInfo,
                        queryManager.getQueryResourceGroup(queryInfo.getQueryId()),
                        queryManager.getQueryResourceGroup(queryInfo.getQueryId()).map(ResourceGroupId::getRoot).map(rootResourceGroupInfos::get)))
                .collect(toImmutableList());
    }
}
