package com.facebook.presto.event.queryinfo;

import com.facebook.presto.execution.FailureInfo;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.StageInfo;
import com.google.inject.Inject;
import io.airlift.json.JsonCodec;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class QueryEventFactory
{
    private final JsonCodec<StageInfo> stageInfoJsonCodec;
    private final JsonCodec<List<FailureInfo>> failureInfoJsonCodec;

    @Inject
    public QueryEventFactory(JsonCodec<StageInfo> stageInfoJsonCodec, JsonCodec<List<FailureInfo>> failureInfoJsonCodec)
    {
        this.stageInfoJsonCodec = checkNotNull(stageInfoJsonCodec, "stageInfoJsonCodec is null");
        this.failureInfoJsonCodec = checkNotNull(failureInfoJsonCodec, "failureInfoJsonCodec is null");
    }

    public QueryCompletionEvent createCompletionEvent(QueryInfo queryInfo)
    {
        return new QueryCompletionEvent(
                queryInfo.getQueryId(),
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
                queryInfo.getQueryStats().getSplits(),
                stageInfoJsonCodec.toJson(queryInfo.getOutputStage()),
                failureInfoJsonCodec.toJson(queryInfo.getFailures())
        );
    }
}
