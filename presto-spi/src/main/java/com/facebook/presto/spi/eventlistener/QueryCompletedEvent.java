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

import java.util.Date;

public class QueryCompletedEvent
{
    private final QueryMetadata queryMetadata;
    private final QueryStatistics queryStatistics;
    private final QueryContext queryContext;
    private final QueryIOMetadata queryIOMetadata;
    private final QueryFailureMetadata queryFailureMetadata;

    private final Date createTime;
    private final Date executionStartTime;
    private final Date endTime;

    public QueryCompletedEvent(QueryMetadata queryMetadata, QueryStatistics queryStatistics, QueryContext queryContext, QueryIOMetadata queryIOMetadata, QueryFailureMetadata queryFailureMetadata, Date createTime, Date executionStartTime, Date endTime)
    {
        this.queryMetadata = queryMetadata;
        this.queryStatistics = queryStatistics;
        this.queryContext = queryContext;
        this.queryIOMetadata = queryIOMetadata;
        this.queryFailureMetadata = queryFailureMetadata;
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.endTime = endTime;
    }

    public QueryMetadata getQueryMetadata()
    {
        return queryMetadata;
    }

    public QueryStatistics getQueryStatistics()
    {
        return queryStatistics;
    }

    public QueryContext getQueryContext()
    {
        return queryContext;
    }

    public QueryIOMetadata getQueryIOMetadata()
    {
        return queryIOMetadata;
    }

    public QueryFailureMetadata getQueryFailureMetadata()
    {
        return queryFailureMetadata;
    }

    public Date getCreateTime()
    {
        return createTime;
    }

    public Date getExecutionStartTime()
    {
        return executionStartTime;
    }

    public Date getEndTime()
    {
        return endTime;
    }
}
