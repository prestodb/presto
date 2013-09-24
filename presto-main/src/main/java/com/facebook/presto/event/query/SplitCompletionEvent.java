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
package com.facebook.presto.event.query;

import com.facebook.presto.execution.QueryId;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.TaskId;
import com.google.common.base.Preconditions;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

@Immutable
@EventType("SplitCompletion")
public class SplitCompletionEvent
{
    private final QueryId queryId;
    private final StageId stageId;
    private final TaskId taskId;

    private final String environment;

    private final Duration queuedTimeMs;
    private final DateTime executionStartTime;

    private final Long timeToFirstByteMs;
    private final Long timeToLastByteMs;

    private final DataSize completedDataSize;
    private final long completedPositions;

    private final Long wallTimeMs;
    private final Long cpuTimeMs;
    private final Long userTimeMs;

    private final String failureType;
    private final String failureMessage;

    private final String splitInfoJson;

    public SplitCompletionEvent(
            QueryId queryId,
            StageId stageId,
            TaskId taskId,
            String environment,
            Duration queuedTimeMs,
            @Nullable DateTime executionStartTime,
            @Nullable Duration timeToFirstByte,
            @Nullable Duration timeToLastByte,
            DataSize completedDataSize,
            long completedPositions,
            @Nullable Duration wallTime,
            @Nullable Duration cpuTime,
            @Nullable Duration userTime,
            @Nullable String failureType,
            @Nullable String failureMessage,
            String splitInfoJson)
    {
        Preconditions.checkNotNull(queryId, "queryId is null");
        Preconditions.checkNotNull(stageId, "stageId is null");
        Preconditions.checkNotNull(taskId, "taskId is null");
        Preconditions.checkNotNull(completedDataSize, "completedDataSize is null");
        Preconditions.checkNotNull(completedPositions, "completedPositions is null");
        Preconditions.checkNotNull(splitInfoJson, "splitInfoJson is null");

        this.queryId = queryId;
        this.stageId = stageId;
        this.taskId = taskId;
        this.environment = environment;
        this.queuedTimeMs = queuedTimeMs;
        this.executionStartTime = executionStartTime;
        this.timeToFirstByteMs = durationToMillis(timeToFirstByte);
        this.timeToLastByteMs = durationToMillis(timeToLastByte);
        this.completedDataSize = completedDataSize;
        this.completedPositions = completedPositions;
        this.wallTimeMs = durationToMillis(wallTime);
        this.cpuTimeMs = durationToMillis(cpuTime);
        this.userTimeMs = durationToMillis(userTime);
        this.failureType = failureType;
        this.failureMessage = failureMessage;
        this.splitInfoJson = splitInfoJson;
    }

    @Nullable
    private static Long durationToMillis(@Nullable Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return duration.toMillis();
    }

    @EventField
    public String getQueryId()
    {
        return queryId.toString();
    }

    @EventField
    public String getStageId()
    {
        return stageId.toString();
    }

    @EventField
    public String getTaskId()
    {
        return taskId.toString();
    }

    @EventField
    public String getEnvironment()
    {
        return environment;
    }

    @EventField
    public long getQueuedTimeMs()
    {
        return queuedTimeMs.toMillis();
    }

    @EventField
    public DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @EventField
    public Long getTimeToFirstByteMs()
    {
        return timeToFirstByteMs;
    }

    @EventField
    public Long getTimeToLastByteMs()
    {
        return timeToLastByteMs;
    }

    @EventField
    public long getCompletedDataSizeTotal()
    {
        return completedDataSize.toBytes();
    }

    @EventField
    public long getCompletedPositionsTotal()
    {
        return completedPositions;
    }

    @EventField
    public Long getWallTimeMs()
    {
        return wallTimeMs;
    }

    @EventField
    public Long getCpuTimeMs()
    {
        return cpuTimeMs;
    }

    @EventField
    public Long getUserTimeMs()
    {
        return userTimeMs;
    }

    @EventField
    public String getFailureType()
    {
        return failureType;
    }

    @EventField
    public String getFailureMessage()
    {
        return failureMessage;
    }

    @EventField
    public String getSplitInfoJson()
    {
        return splitInfoJson;
    }
}
