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

import com.facebook.presto.common.resourceGroups.QueryType;

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryProgressEvent
{
    private final long monotonicallyIncreasingEventId;
    private final QueryMetadata metadata;
    private final QueryStatistics statistics;
    private final QueryContext context;
    private final Optional<QueryType> queryType;
    private final Instant createTime;

    public QueryProgressEvent(
            long monotonicallyIncreasingEventId,
            QueryMetadata metadata,
            QueryStatistics statistics,
            QueryContext context,
            Optional<QueryType> queryType,
            Instant createTime)
    {
        this.monotonicallyIncreasingEventId = monotonicallyIncreasingEventId;
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.context = requireNonNull(context, "context is null");
        this.queryType = requireNonNull(queryType, "queryType is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
    }

    public long getMonotonicallyIncreasingEventId()
    {
        return monotonicallyIncreasingEventId;
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

    public Optional<QueryType> getQueryType()
    {
        return queryType;
    }

    public Instant getCreateTime()
    {
        return createTime;
    }
}
