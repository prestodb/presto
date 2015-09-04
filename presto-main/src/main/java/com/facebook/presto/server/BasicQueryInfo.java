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

import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.operator.BlockedReason;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.StandardErrorCode.ErrorType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.net.URI;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class BasicQueryInfo
{
    private final QueryId queryId;
    private final SessionRepresentation session;
    private final QueryState state;
    private final ErrorType errorType;
    private final ErrorCode errorCode;
    private final boolean scheduled;
    private final boolean fullyBlocked;
    private final Set<BlockedReason> blockedReasons;
    private final URI self;
    private final String query;
    private final Duration elapsedTime;
    private final DateTime endTime;
    private final DateTime createTime;
    private final int runningDrivers;
    private final int queuedDrivers;
    private final int completedDrivers;
    private final int totalDrivers;

    @JsonCreator
    public BasicQueryInfo(
            @JsonProperty("queryId") QueryId queryId,
            @JsonProperty("session") SessionRepresentation session,
            @JsonProperty("state") QueryState state,
            @JsonProperty("errorType") ErrorType errorType,
            @JsonProperty("errorCode") ErrorCode errorCode,
            @JsonProperty("scheduled") boolean scheduled,
            @JsonProperty("fullyBlocked") boolean fullyBlocked,
            @JsonProperty("blockedReasons") Set<BlockedReason> blockedReasons,
            @JsonProperty("self") URI self,
            @JsonProperty("query") String query,
            @JsonProperty("elapsedTime") Duration elapsedTime,
            @JsonProperty("endTime") DateTime endTime,
            @JsonProperty("createTime") DateTime createTime,
            @JsonProperty("runningDrivers") int runningDrivers,
            @JsonProperty("queuedDrivers") int queuedDrivers,
            @JsonProperty("completedDrivers") int completedDrivers,
            @JsonProperty("totalDrivers") int totalDrivers)

    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.session = requireNonNull(session, "session is null");
        this.state = requireNonNull(state, "state is null");
        this.errorType = errorType;
        this.errorCode = errorCode;
        this.scheduled = scheduled;
        this.fullyBlocked = fullyBlocked;
        this.blockedReasons = ImmutableSet.copyOf(requireNonNull(blockedReasons, "blockedReasons is null"));
        this.self = requireNonNull(self, "self is null");
        this.query = requireNonNull(query, "query is null");
        this.elapsedTime = elapsedTime;
        this.endTime = endTime;
        this.createTime = createTime;

        checkArgument(runningDrivers >= 0, "runningDrivers is less than zero");
        this.runningDrivers = runningDrivers;
        checkArgument(queuedDrivers >= 0, "queuedDrivers is less than zero");
        this.queuedDrivers = queuedDrivers;
        checkArgument(completedDrivers >= 0, "completedDrivers is less than zero");
        this.completedDrivers = completedDrivers;
        checkArgument(totalDrivers >= 0, "totalDrivers is less than zero");
        this.totalDrivers = totalDrivers;
    }

    public BasicQueryInfo(QueryInfo queryInfo)
    {
        this(queryInfo.getQueryId(),
                queryInfo.getSession(),
                queryInfo.getState(),
                queryInfo.getErrorType(),
                queryInfo.getErrorCode(),
                queryInfo.isScheduled(),
                queryInfo.getQueryStats().isFullyBlocked(),
                queryInfo.getQueryStats().getBlockedReasons(),
                queryInfo.getSelf(),
                queryInfo.getQuery(),
                queryInfo.getQueryStats().getElapsedTime(),
                queryInfo.getQueryStats().getEndTime(),
                queryInfo.getQueryStats().getCreateTime(),
                queryInfo.getQueryStats().getRunningDrivers(),
                queryInfo.getQueryStats().getQueuedDrivers(),
                queryInfo.getQueryStats().getCompletedDrivers(),
                queryInfo.getQueryStats().getTotalDrivers());
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
    public QueryState getState()
    {
        return state;
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

    @JsonProperty
    public boolean isScheduled()
    {
        return scheduled;
    }

    @JsonProperty
    public boolean isFullyBlocked()
    {
        return fullyBlocked;
    }

    @JsonProperty
    public Set<BlockedReason> getBlockedReasons()
    {
        return blockedReasons;
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
    public Duration getElapsedTime()
    {
        return elapsedTime;
    }

    @JsonProperty
    public DateTime getEndTime()
    {
        return endTime;
    }

    @JsonProperty
    public int getRunningDrivers()
    {
        return runningDrivers;
    }

    @JsonProperty
    public int getQueuedDrivers()
    {
        return queuedDrivers;
    }

    @JsonProperty
    public int getTotalDrivers()
    {
        return totalDrivers;
    }

    @JsonProperty
    public int getCompletedDrivers()
    {
        return completedDrivers;
    }

    @JsonProperty
    public DateTime getCreateTime()
    {
        return createTime;
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
