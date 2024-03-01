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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCode;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.execution.QueryState.QUEUED;
import static com.facebook.presto.server.QueryProgressStats.createQueryProgressStats;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class QueryStateInfo
{
    private final QueryState queryState;
    private final QueryId queryId;
    private final Optional<ResourceGroupId> resourceGroupId;
    private final String query;
    private final boolean queryTruncated;
    private final DateTime createTime;
    private final String user;
    private final boolean authenticated;
    private final Optional<String> source;
    private final Optional<String> clientInfo;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final Optional<List<ResourceGroupInfo>> pathToRoot;
    private final Optional<QueryProgressStats> progress;
    private final List<String> warningCodes;
    private final Optional<ErrorCode> errorCode;

    @JsonCreator
    @ThriftConstructor
    public QueryStateInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("queryState") QueryState queryState,
            @JsonProperty("resourceGroupId") Optional<ResourceGroupId> resourceGroupId,
            @JsonProperty("query") String query,
            @JsonProperty("queryTruncated") boolean queryTruncated,
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("user") String user,
            @JsonProperty("authenticated") boolean authenticated,
            @JsonProperty("source") Optional<String> source,
            @JsonProperty("clientInfo") Optional<String> clientInfo,
            @JsonProperty("catalog") Optional<String> catalog,
            @JsonProperty("schema") Optional<String> schema,
            @JsonProperty("pathToRoot") Optional<List<ResourceGroupInfo>> pathToRoot,
            @JsonProperty("progress") Optional<QueryProgressStats> progress,
            @JsonProperty("warningCodes") List<String> warningCodes,
            @JsonProperty("errorCode") Optional<ErrorCode> errorCode)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.query = requireNonNull(query, "query text is null");
        this.queryTruncated = queryTruncated;
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.user = requireNonNull(user, "user is null");
        this.authenticated = authenticated;
        this.source = requireNonNull(source, "source is null");
        this.clientInfo = requireNonNull(clientInfo, "clientInfo is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        requireNonNull(pathToRoot, "pathToRoot is null");
        this.pathToRoot = pathToRoot.map(ImmutableList::copyOf);
        this.progress = requireNonNull(progress, "progress is null");
        this.warningCodes = ImmutableList.copyOf(requireNonNull(warningCodes, "warningCodes is null"));
        this.errorCode = requireNonNull(errorCode, "errorCode is null");
    }

    public static QueryStateInfo createQueryStateInfo(BasicQueryInfo queryInfo)
    {
        return createQueryStateInfo(queryInfo, Optional.empty(), false, OptionalInt.empty());
    }

    public static QueryStateInfo createQueryStateInfo(
            BasicQueryInfo queryInfo,
            Optional<List<ResourceGroupInfo>> pathToRoot,
            boolean includeAllQueryProgressStats,
            OptionalInt queryTextSizeLimit)
    {
        Optional<QueryProgressStats> progress = Optional.empty();
        if (includeAllQueryProgressStats || (!queryInfo.getState().isDone() && queryInfo.getState() != QUEUED)) {
            progress = Optional.of(createQueryProgressStats(queryInfo.getQueryStats()));
        }
        return createQueryStateInfo(queryInfo, pathToRoot, progress, queryTextSizeLimit);
    }

    private static QueryStateInfo createQueryStateInfo(
            BasicQueryInfo queryInfo,
            Optional<List<ResourceGroupInfo>> pathToRoot,
            Optional<QueryProgressStats> progress,
            OptionalInt queryTextSizeLimit)
    {
        String query = queryInfo.getQuery();
        boolean queryTruncated = false;

        if (queryTextSizeLimit.isPresent() && queryInfo.getQuery().length() > queryTextSizeLimit.getAsInt()) {
            query = query.substring(0, queryTextSizeLimit.getAsInt());
            queryTruncated = true;
        }

        return new QueryStateInfo(
                queryInfo.getQueryId(),
                queryInfo.getState(),
                queryInfo.getResourceGroupId(),
                query,
                queryTruncated,
                queryInfo.getQueryStats().getCreateTime(),
                queryInfo.getSession().getUser(),
                queryInfo.getSession().getPrincipal().isPresent(),
                queryInfo.getSession().getSource(),
                queryInfo.getSession().getClientInfo(),
                queryInfo.getSession().getCatalog(),
                queryInfo.getSession().getSchema(),
                pathToRoot,
                progress,
                queryInfo.getWarnings().stream().map(PrestoWarning::getWarningCode).map(WarningCode::getName).collect(toImmutableList()),
                Optional.ofNullable(queryInfo.getErrorCode()));
    }

    @JsonProperty
    @ThriftField(1)
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    @ThriftField(2)
    public QueryState getQueryState()
    {
        return queryState;
    }

    @JsonProperty
    @ThriftField(3)
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    @ThriftField(4)
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    @ThriftField(5)
    public boolean isQueryTruncated()
    {
        return queryTruncated;
    }

    @JsonProperty
    @ThriftField(6)
    public String getUser()
    {
        return user;
    }

    @JsonProperty
    @ThriftField(7)
    public boolean isAuthenticated()
    {
        return authenticated;
    }

    @JsonProperty
    @ThriftField(8)
    public Optional<String> getSource()
    {
        return source;
    }

    @JsonProperty
    @ThriftField(9)
    public Optional<String> getClientInfo()
    {
        return clientInfo;
    }

    @JsonProperty
    @ThriftField(10)
    public Optional<String> getCatalog()
    {
        return catalog;
    }

    @JsonProperty
    @ThriftField(11)
    public Optional<String> getSchema()
    {
        return schema;
    }

    @JsonProperty
    @ThriftField(12)
    public Optional<List<ResourceGroupInfo>> getPathToRoot()
    {
        return pathToRoot;
    }

    @JsonProperty
    @ThriftField(13)
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @JsonProperty
    @ThriftField(14)
    public Optional<QueryProgressStats> getProgress()
    {
        return progress;
    }

    @JsonProperty
    @ThriftField(15)
    public List<String> getWarningCodes()
    {
        return warningCodes;
    }

    @JsonProperty
    @ThriftField(16)
    public Optional<ErrorCode> getErrorCode()
    {
        return errorCode;
    }
}
