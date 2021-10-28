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
package com.facebook.presto.spi.eventlistener;

import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.resourceGroups.QueryType;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryCompletedEvent
{
    private final QueryMetadata metadata;
    private final QueryStatistics statistics;
    private final QueryContext context;
    private final QueryIOMetadata ioMetadata;
    private final Optional<QueryFailureInfo> failureInfo;
    private final List<PrestoWarning> warnings;
    private final Optional<QueryType> queryType;
    private final List<String> failedTasks;
    private final List<StageStatistics> stageStatistics;
    private final List<OperatorStatistics> operatorStatistics;

    private final Instant createTime;
    private final Instant executionStartTime;
    private final Instant endTime;
    private final Optional<String> expandedQuery;

    public QueryCompletedEvent(
            QueryMetadata metadata,
            QueryStatistics statistics,
            QueryContext context,
            QueryIOMetadata ioMetadata,
            Optional<QueryFailureInfo> failureInfo,
            List<PrestoWarning> warnings,
            Optional<QueryType> queryType,
            List<String> failedTasks,
            Instant createTime,
            Instant executionStartTime,
            Instant endTime,
            List<StageStatistics> stageStatistics,
            List<OperatorStatistics> operatorStatistics,
            Optional<String> expandedQuery)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.context = requireNonNull(context, "context is null");
        this.ioMetadata = requireNonNull(ioMetadata, "ioMetadata is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.warnings = requireNonNull(warnings, "queryWarnings is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.failedTasks = requireNonNull(failedTasks, "failedTasks is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = requireNonNull(executionStartTime, "executionStartTime is null");
        this.endTime = requireNonNull(endTime, "endTime is null");
        this.stageStatistics = requireNonNull(stageStatistics, "stageStatistics is null");
        this.operatorStatistics = requireNonNull(operatorStatistics, "operatorStatistics is null");
        this.expandedQuery = requireNonNull(expandedQuery, "expandedQuery is null");
    }

    public QueryMetadata getMetadata()
    {
        return metadata;
    }

    public QueryStatistics getStatistics()
    {
        return statistics;
    }

    public QueryContext getContext()
    {
        return context;
    }

    public QueryIOMetadata getIoMetadata()
    {
        return ioMetadata;
    }

    public Optional<QueryFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    public List<PrestoWarning> getWarnings()
    {
        return warnings;
    }

    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    public List<String> getFailedTasks()
    {
        return failedTasks;
    }

    public Instant getCreateTime()
    {
        return createTime;
    }

    public Instant getExecutionStartTime()
    {
        return executionStartTime;
    }

    public Instant getEndTime()
    {
        return endTime;
    }

    public List<StageStatistics> getStageStatistics()
    {
        return stageStatistics;
    }

    public List<OperatorStatistics> getOperatorStatistics()
    {
        return operatorStatistics;
    }

    public Optional<String> getExpandedQuery()
    {
        return expandedQuery;
    }
}
