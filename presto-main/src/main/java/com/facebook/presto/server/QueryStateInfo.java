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
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.facebook.presto.spi.resourceGroups.ResourceGroupInfo;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.server.QueryProgressStats.createQueryProgressStats;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class QueryStateInfo
{
    private final QueryState queryState;
    private final QueryId queryId;
    private final Optional<ResourceGroupId> resourceGroupId;
    private final String query;
    private final DateTime createTime;
    private final String user;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<List<ResourceGroupInfo>> resourceGroupChain;
    private final Optional<QueryProgressStats> progress;

    @JsonCreator
    private QueryStateInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("queryState") QueryState queryState,
            @JsonProperty("resourceGroupId") Optional<ResourceGroupId> resourceGroupId,
            @JsonProperty("query") String query,
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("user") String user,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("resourceGroupChainInfo") Optional<List<ResourceGroupInfo>> resourceGroupChain,
            @JsonProperty("progress") Optional<QueryProgressStats> progress)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.query = requireNonNull(query, "query text is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.user = requireNonNull(user, "user is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        requireNonNull(resourceGroupChain, "resourceGroupChain is null");
        this.resourceGroupChain = resourceGroupChain.map(ImmutableList::copyOf);
        this.progress = requireNonNull(progress, "progress is null");
    }

    public static QueryStateInfo createQueryStateInfo(QueryInfo queryInfo, Optional<ResourceGroupId> resourceGroupId, Optional<ResourceGroupInfo> rootResourceGroupInfo)
    {
        Optional<List<ResourceGroupInfo>> resourceGroups = Optional.empty();
        Optional<QueryProgressStats> progress = Optional.empty();
        if (queryInfo.getState() == QUEUED) {
            resourceGroups = Optional.of(ImmutableList.of());
            if (resourceGroupId.isPresent() && rootResourceGroupInfo.isPresent()) {
                ImmutableList.Builder<ResourceGroupInfo> builder = ImmutableList.builder();
                ResourceGroupId id = resourceGroupId.get();
                ResourceGroupInfo resourceGroupInfo = rootResourceGroupInfo.get();

                while (true) {
                    builder.add(resourceGroupInfo.createSingleNodeInfo());

                    if (resourceGroupInfo.getSubGroups().isEmpty()) {
                        break;
                    }

                    Optional<ResourceGroupInfo> subGroupInfo = resourceGroupInfo.getSubGroup(id);
                    checkArgument(subGroupInfo.isPresent(), "No path from root resource group %s to resource group %s", rootResourceGroupInfo.get().getId(), id);
                    resourceGroupInfo = subGroupInfo.get();
                }

                resourceGroups = Optional.of(builder.build().reverse());
            }
        }
        else if (queryInfo.getState() == RUNNING) {
            progress = Optional.of(createQueryProgressStats(queryInfo.getQueryStats()));
        }

        return new QueryStateInfo(
                queryInfo.getQueryId(),
                queryInfo.getState(),
                resourceGroupId,
                queryInfo.getQuery(),
                queryInfo.getQueryStats().getCreateTime(),
                queryInfo.getSession().getUser(),
                queryInfo.getSession().getCatalog(),
                queryInfo.getSession().getSchema(),
                resourceGroups,
                progress);
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public QueryState getQueryState()
    {
        return queryState;
    }

    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Optional<List<ResourceGroupInfo>> getResourceGroupChain()
    {
        return resourceGroupChain;
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    public Optional<QueryProgressStats> getProgress()
    {
        return progress;
    }
}
