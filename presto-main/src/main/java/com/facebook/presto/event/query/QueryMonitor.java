package com.facebook.presto.event.query;

import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.TaskInfo;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.airlift.event.client.EventClient;

import static com.facebook.presto.execution.StageInfo.globalExecutionStats;
import static com.facebook.presto.operator.OperatorStats.SplitExecutionStats;
import static com.google.common.base.Preconditions.checkNotNull;

public class QueryMonitor
{
    private final ObjectMapper objectMapper;
    private final EventClient eventClient;

    @Inject
    public QueryMonitor(ObjectMapper objectMapper, EventClient eventClient)
    {
        this.objectMapper = checkNotNull(objectMapper, "objectMapper is null");
        this.eventClient = checkNotNull(eventClient, "eventClient is null");
    }

    public void createdEvent(QueryInfo queryInfo)
    {
        eventClient.post(
                new QueryCreatedEvent(
                        queryInfo.getQueryId(),
                        queryInfo.getSession().getUser(),
                        queryInfo.getSession().getCatalog(),
                        queryInfo.getSession().getSchema(),
                        queryInfo.getSelf(),
                        queryInfo.getQuery(),
                        queryInfo.getQueryStats().getCreateTime()
                )
        );
    }

    public void completionEvent(QueryInfo queryInfo)
    {
        try {
            ExecutionStats globalExecutionStats = globalExecutionStats(queryInfo.getOutputStage());
            eventClient.post(
                    new QueryCompletionEvent(
                            queryInfo.getQueryId(),
                            queryInfo.getSession().getUser(),
                            queryInfo.getSession().getCatalog(),
                            queryInfo.getSession().getSchema(),
                            queryInfo.getState(),
                            queryInfo.getSelf(),
                            queryInfo.getFieldNames(),
                            queryInfo.getQuery(),
                            queryInfo.getQueryStats().getCreateTime(),
                            queryInfo.getQueryStats().getExecutionStartTime(),
                            queryInfo.getQueryStats().getEndTime(),
                            queryInfo.getQueryStats().getQueuedTime(),
                            queryInfo.getQueryStats().getAnalysisTime(),
                            queryInfo.getQueryStats().getDistributedPlanningTime(),
                            globalExecutionStats.getSplitWallTime(),
                            globalExecutionStats.getSplitCpuTime(),
                            globalExecutionStats.getCompletedDataSize(),
                            globalExecutionStats.getCompletedPositionCount(),
                            globalExecutionStats.getSplits(),
                            objectMapper.writeValueAsString(queryInfo.getOutputStage()),
                            objectMapper.writeValueAsString(queryInfo.getFailureInfo())
                    )
            );
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }

    public void splitCompletionEvent(TaskInfo taskInfo, SplitExecutionStats splitExecutionStats)
    {
        try {
            eventClient.post(
                    new SplitCompletionEvent(
                            taskInfo.getTaskId().getQueryId(),
                            taskInfo.getTaskId().getStageId(),
                            taskInfo.getTaskId(),
                            splitExecutionStats.getExecutionStartTime(),
                            splitExecutionStats.getTimeToFirstByte(),
                            splitExecutionStats.getTimeToLastByte(),
                            splitExecutionStats.getCompletedDataSize(),
                            splitExecutionStats.getCompletedPositions(),
                            splitExecutionStats.getWall(),
                            splitExecutionStats.getCpu(),
                            splitExecutionStats.getUser(),
                            objectMapper.writeValueAsString(splitExecutionStats.getSplitInfo())
                    )
            );
        }
        catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        }
    }
}
