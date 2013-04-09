package com.facebook.presto.event.query;

import com.google.common.base.Preconditions;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import static com.facebook.presto.operator.OperatorStats.SmallCounterStat.SmallCounterStatSnapshot;

@Immutable
@EventType("SplitCompletion")
public class SplitCompletionEvent
{
    private final String queryId;
    private final String stageId;
    private final String taskId;

    private final DateTime executionStartTime;

    private final Long timeToFirstByteMs;
    private final Long timeToLastByteMs;

    private final SmallCounterStatSnapshot completedDataSize;
    private final SmallCounterStatSnapshot completedPositions;

    private final Long wallTimeMs;
    private final Long cpuTimeMs;
    private final Long userTimeMs;

    private final String splitInfoJson;

    public SplitCompletionEvent(
            String queryId,
            String stageId,
            String taskId,
            @Nullable DateTime executionStartTime,
            @Nullable Duration timeToFirstByte,
            @Nullable Duration timeToLastByte,
            SmallCounterStatSnapshot completedDataSize,
            SmallCounterStatSnapshot completedPositions,
            @Nullable Duration wallTime,
            @Nullable Duration cpuTime,
            @Nullable Duration userTime,
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
        this.executionStartTime = executionStartTime;
        this.timeToFirstByteMs = durationToMillis(timeToFirstByte);
        this.timeToLastByteMs = durationToMillis(timeToLastByte);
        this.completedDataSize = completedDataSize;
        this.completedPositions = completedPositions;
        this.wallTimeMs = durationToMillis(wallTime);
        this.cpuTimeMs = durationToMillis(cpuTime);
        this.userTimeMs = durationToMillis(userTime);
        this.splitInfoJson = splitInfoJson;
    }

    @Nullable
    private static Long durationToMillis(@Nullable Duration duration)
    {
        if (duration == null) {
            return null;
        }
        return (long) duration.toMillis();
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public String getStageId()
    {
        return stageId;
    }

    @EventField
    public String getTaskId()
    {
        return taskId;
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
        return completedDataSize.getTotalCount();
    }

    @EventField
    public double getCompletedDataSizeCountTenSec()
    {
        return completedDataSize.getTenSeconds().getCount();
    }

    @EventField
    public double getCompletedDataSizeRateTenSec()
    {
        return completedDataSize.getTenSeconds().getRate();
    }

    @EventField
    public double getCompletedDataSizeCountThirtySec()
    {
        return completedDataSize.getThirtySeconds().getCount();
    }

    @EventField
    public double getCompletedDataSizeRateThirtySec()
    {
        return completedDataSize.getThirtySeconds().getRate();
    }

    @EventField
    public double getCompletedDataSizeCountOneMin()
    {
        return completedDataSize.getOneMinute().getCount();
    }

    @EventField
    public double getCompletedDataSizeRateOneMin()
    {
        return completedDataSize.getOneMinute().getRate();
    }

    @EventField
    public long getCompletedPositionsTotal()
    {
        return completedPositions.getTotalCount();
    }

    @EventField
    public double getCompletedPositionsCountTenSec()
    {
        return completedPositions.getTenSeconds().getCount();
    }

    @EventField
    public double getCompletedPositionsRateTenSec()
    {
        return completedPositions.getTenSeconds().getRate();
    }

    @EventField
    public double getCompletedPositionsCountThirtySec()
    {
        return completedPositions.getThirtySeconds().getCount();
    }

    @EventField
    public double getCompletedPositionsRateThirtySec()
    {
        return completedPositions.getThirtySeconds().getRate();
    }

    @EventField
    public double getCompletedPositionsCountOneMin()
    {
        return completedPositions.getOneMinute().getCount();
    }

    @EventField
    public double getCompletedPositionsRateOneMin()
    {
        return completedPositions.getOneMinute().getRate();
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
    public String getSplitInfoJson()
    {
        return splitInfoJson;
    }
}
