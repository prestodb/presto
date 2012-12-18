package com.facebook.presto.event.queryinfo;

import com.facebook.presto.execution.QueryState;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;

/**
 * Flattened Event version of QueryInfo at query completion
 */
@Immutable
@EventType("QueryCompletion")
public class QueryCompletionEvent
{
    private final String queryId;
    private final QueryState queryState;
    private final URI uri;
    private final List<String> fieldNames;
    private final String query;

    private final DateTime createTime;
    private final DateTime executionStartTime;

    private final DateTime endTime;
    // times are in ms
    private final long queuedTimeMs;
    private final long analysisTimeMs;
    private final long distributedPlanningTimeMs;
    private final int splits;

    private final String outputStageJson;
    private final String failuresJson;

    public QueryCompletionEvent(
            String queryId,
            QueryState queryState,
            URI uri,
            List<String> fieldNames,
            String query,
            DateTime createTime,
            DateTime executionStartTime,
            DateTime endTime,
            long queuedTimeMs,
            long analysisTimeMs,
            long distributedPlanningTimeMs,
            int splits,
            String outputStageJson,
            String failuresJson)
    {
        this.queryId = queryId;
        this.queryState = queryState;
        this.uri = uri;
        this.fieldNames = fieldNames;
        this.query = query;
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.endTime = endTime;
        this.queuedTimeMs = queuedTimeMs;
        this.analysisTimeMs = analysisTimeMs;
        this.distributedPlanningTimeMs = distributedPlanningTimeMs;
        this.splits = splits;
        this.outputStageJson = outputStageJson;
        this.failuresJson = failuresJson;
    }

    @EventField
    public String getQueryId()
    {
        return queryId;
    }

    @EventField
    public String getQueryState()
    {
        return queryState.name();
    }

    @EventField
    public String getUri()
    {
        return uri.toString();
    }

    @EventField
    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    @EventField
    public String getQuery()
    {
        return query;
    }

    @EventField
    public DateTime getCreateTime()
    {
        return createTime;
    }

    @EventField
    public DateTime getExecutionStartTime()
    {
        return executionStartTime;
    }

    @EventField
    public DateTime getEndTime()
    {
        return endTime;
    }

    @EventField
    public long getQueuedTimeMs()
    {
        return queuedTimeMs;
    }

    @EventField
    public long getAnalysisTimeMs()
    {
        return analysisTimeMs;
    }

    @EventField
    public long getDistributedPlanningTimeMs()
    {
        return distributedPlanningTimeMs;
    }

    @EventField
    public int getSplits()
    {
        return splits;
    }

    @EventField
    public String getOutputStageJson()
    {
        return outputStageJson;
    }

    @EventField
    public String getFailuresJson()
    {
        return failuresJson;
    }
}
