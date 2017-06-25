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

import java.time.Instant;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class SplitCompletedEvent
{
    private final String queryId;
    private final String stageId;
    private final String taskId;

    private final Instant createTime;
    private final Optional<Instant> startTime;
    private final Optional<Instant> endTime;

    private final SplitStatistics statistics;
    private final Optional<SplitFailureInfo> failureInfo;

    private final String payload;

    public SplitCompletedEvent(
            String queryId,
            String stageId,
            String taskId,
            Instant createTime,
            Optional<Instant> startTime,
            Optional<Instant> endTime,
            SplitStatistics statistics,
            Optional<SplitFailureInfo> failureInfo,
            String payload)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.taskId = requireNonNull(taskId, "taskId is null");
        this.createTime = requireNonNull(createTime, "createTime is null");
        this.startTime = requireNonNull(startTime, "startTime is null");
        this.endTime = requireNonNull(endTime, "endTime is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
        this.failureInfo = requireNonNull(failureInfo, "failureInfo is null");
        this.payload = requireNonNull(payload, "payload is null");
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getStageId()
    {
        return stageId;
    }

    public String getTaskId()
    {
        return taskId;
    }

    public Instant getCreateTime()
    {
        return createTime;
    }

    public Optional<Instant> getStartTime()
    {
        return startTime;
    }

    public Optional<Instant> getEndTime()
    {
        return endTime;
    }

    public SplitStatistics getStatistics()
    {
        return statistics;
    }

    public Optional<SplitFailureInfo> getFailureInfo()
    {
        return failureInfo;
    }

    public String getPayload()
    {
        return payload;
    }
}
