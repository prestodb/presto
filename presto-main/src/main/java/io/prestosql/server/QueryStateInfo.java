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
package io.prestosql.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.execution.QueryState;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;

import static io.prestosql.execution.QueryState.QUEUED;
import static io.prestosql.server.QueryProgressStats.createQueryProgressStats;
import static java.util.Objects.requireNonNull;

public class QueryStateInfo
{
    private final QueryState queryState;
    private final QueryId queryId;
    private final Optional<ResourceGroupId> resourceGroupId;
    private final String query;
    private final DateTime createTime;
    private final String user;
    private final Optional<String> source;
    private final Optional<String> clientInfo;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<List<ResourceGroupInfo>> pathToRoot;
    private final Optional<QueryProgressStats> progress;

    @JsonCreator
    public QueryStateInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("queryState") QueryState queryState,
            @JsonProperty("resourceGroupId") Optional<ResourceGroupId> resourceGroupId,
            @JsonProperty("query") String query,
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("user") String user,
            @JsonProperty("source") Optional<String> source,
            @JsonProperty("clientInfo") Optional<String> clientInfo,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("pathToRoot") Optional<List<ResourceGroupInfo>> pathToRoot,
            @JsonProperty("progress") Optional<QueryProgressStats> progress)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.query = requireNonNull(query, "query text is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.user = requireNonNull(user, "user is null");
        this.source = requireNonNull(source, "source is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        requireNonNull(pathToRoot, "pathToRoot is null");
        this.pathToRoot = pathToRoot.map(ImmutableList::copyOf);
        this.progress = requireNonNull(progress, "progress is null");
    }

    public static QueryStateInfo createQueuedQueryStateInfo(BasicQueryInfo queryInfo, Optional<ResourceGroupId> group, Optional<List<ResourceGroupInfo>> pathToRoot)
    {
        return createQueryStateInfo(queryInfo, group, pathToRoot, Optional.empty());
    }

    public static QueryStateInfo createQueryStateInfo(BasicQueryInfo queryInfo, Optional<ResourceGroupId> group)
    {
        Optional<QueryProgressStats> progress = Optional.empty();
        if (!queryInfo.getState().isDone() && queryInfo.getState() != QUEUED) {
            progress = Optional.of(createQueryProgressStats(queryInfo.getQueryStats()));
        }
        return createQueryStateInfo(queryInfo, group, Optional.empty(), progress);
    }

    private static QueryStateInfo createQueryStateInfo(
            BasicQueryInfo queryInfo,
            Optional<ResourceGroupId> groupId,
            Optional<List<ResourceGroupInfo>> pathToRoot,
            Optional<QueryProgressStats> progress)
    {
        return new QueryStateInfo(
                queryInfo.getQueryId(),
                queryInfo.getState(),
                groupId,
                queryInfo.getQuery(),
                queryInfo.getQueryStats().getCreateTime(),
                queryInfo.getSession().getUser(),
                queryInfo.getSession().getSource(),
                queryInfo.getSession().getClientInfo(),
                queryInfo.getSession().getCatalog(),
                queryInfo.getSession().getSchema(),
                pathToRoot,
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
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    public Optional<String> getClientInfo()
    {
        return clientInfo;
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
    public Optional<List<ResourceGroupInfo>> getPathToRoot()
    {
        return pathToRoot;
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
