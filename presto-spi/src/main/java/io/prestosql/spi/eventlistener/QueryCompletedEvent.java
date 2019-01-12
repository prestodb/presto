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
package io.prestosql.spi.eventlistener;

import io.prestosql.spi.PrestoWarning;

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

    private final Instant createTime;
    private final Instant executionStartTime;
    private final Instant endTime;

    public QueryCompletedEvent(
            QueryMetadata metadata,
            QueryStatistics statistics,
            QueryContext context,
            QueryIOMetadata ioMetadata,
            Optional<QueryFailureInfo> failureInfo,
            List<PrestoWarning> warnings,
            Instant createTime,
            Instant executionStartTime,
            Instant endTime)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.context = requireNonNull(context, "context is null");
        this.ioMetadata = requireNonNull(ioMetadata, "ioMetadata is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.warnings = requireNonNull(warnings, "queryWarnings is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.executionStartTime = requireNonNull(executionStartTime, "executionStartTime is null");
        this.endTime = requireNonNull(endTime, "endTime is null");
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
}
