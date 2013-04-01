package com.facebook.presto.event.query;

import com.facebook.presto.execution.ExecutionStats;
import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.google.inject.Inject;
import io.airlift.event.client.EventClient;
import io.airlift.json.JsonCodec;

import static com.facebook.presto.execution.StageInfo.globalExecutionStats;
import static com.google.common.base.Preconditions.checkNotNull;

public class QueryMonitor
{
    private final JsonCodec<StageInfo> stageInfoCodec;
    private final JsonCodec<FailureInfo> failureInfoCodec;
    private final EventClient eventClient;

    @Inject
    public QueryMonitor(JsonCodec<StageInfo> stageInfoCodec, JsonCodec<FailureInfo> failureInfoCodec, EventClient eventClient)
    {
        this.stageInfoCodec = checkNotNull(stageInfoCodec, "stageInfoCodec is null");
        this.failureInfoCodec = checkNotNull(failureInfoCodec, "failureInfoCodec is null");
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
                        stageInfoCodec.toJson(queryInfo.getOutputStage()),
                        failureInfoCodec.toJson(queryInfo.getFailureInfo())
                )
        );
    }
}
