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
import io.prestosql.SessionRepresentation;
import io.prestosql.execution.QueryInfo;
import io.prestosql.execution.QueryState;
import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorType;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spi.resourcegroups.ResourceGroupId;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

/**
 * Lightweight version of QueryInfo. Parts of the web UI depend on the fields
 * being named consistently across these classes.
 */
@Immutable
public class BasicQueryInfo
{
    private final QueryId queryId;
    private final SessionRepresentation session;
    private final Optional<ResourceGroupId> resourceGroupId;
    private final QueryState state;
    private final MemoryPoolId memoryPool;
    private final boolean scheduled;
    private final URI self;
    private final String query;
    private final BasicQueryStats queryStats;
    private final ErrorType errorType;
    private final ErrorCode errorCode;

    @JsonCreator
    public BasicQueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("resourceGroupId") Optional<ResourceGroupId> resourceGroupId,
            @JsonProperty("state") QueryState state,
            @JsonProperty("memoryPool") MemoryPoolId memoryPool,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("self") URI self,
            @JsonProperty("query") String query,
            @JsonProperty("queryStats") BasicQueryStats queryStats,
            @JsonProperty("errorType") ErrorType errorType,
            @JsonProperty("errorCode") ErrorCode errorCode)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.session = requireNonNull(session, "session is null");
        this.resourceGroupId = requireNonNull(resourceGroupId, "resourceGroupId is null");
        this.state = requireNonNull(state, "state is null");
        this.memoryPool = memoryPool;
        this.errorType = errorType;
        this.errorCode = errorCode;
        this.scheduled = scheduled;
        this.self = requireNonNull(self, "self is null");
        this.query = requireNonNull(query, "query is null");
        this.queryStats = requireNonNull(queryStats, "queryStats is null");
    }

    public BasicQueryInfo(QueryInfo queryInfo)
    {
        this(queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getResourceGroupId(),
                queryInfo.getState(),
                queryInfo.getMemoryPool(),
                queryInfo.isScheduled(),
                queryInfo.getSelf(),
                queryInfo.getQuery(),
                new BasicQueryStats(queryInfo.getQueryStats()),
                queryInfo.getErrorType(),
                queryInfo.getErrorCode());
    }

    @JsonProperty
    public QueryId getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public SessionRepresentation getSession()
    {
        return session;
    }

    @JsonProperty
    public Optional<ResourceGroupId> getResourceGroupId()
    {
        return resourceGroupId;
    }

    @JsonProperty
    public QueryState getState()
    {
        return state;
    }

    @JsonProperty
    public MemoryPoolId getMemoryPool()
    {
        return memoryPool;
    }

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public URI getSelf()
    {
        return self;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public BasicQueryStats getQueryStats()
    {
        return queryStats;
    }

    @Nullable
    @JsonProperty
    public ErrorType getErrorType()
    {
        return errorType;
    }

    @Nullable
    @JsonProperty
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryId", queryId)
                .add("state", state)
                .toString();
    }
}
