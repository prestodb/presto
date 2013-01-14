package com.facebook.presto.event.query;

import com.facebook.presto.execution.QueryState;
import com.google.common.collect.ImmutableList;
import io.airlift.event.client.EventField;
import io.airlift.event.client.EventType;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;

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
    private final Long queuedTimeMs;
    private final Long analysisTimeMs;
    private final Long distributedPlanningTimeMs;
    private final Long totalSplitWallTimeMs;
    private final Long totalSplitCpuTimeMs;
    private final Long totalBytes;
    private final Long totalRows;

    private final Integer splits;

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
            Long queuedTimeMs,
            Long analysisTimeMs,
            Long distributedPlanningTimeMs,
            Long totalSplitWallTimeMs,
            Long totalSplitCpuTimeMs,
            Long totalBytes,
            Long totalRows,
            Integer splits,
            String outputStageJson,
            String failuresJson)
    {
        this.queryId = queryId;
        this.queryState = queryState;
        this.uri = uri;
        this.fieldNames = ImmutableList.copyOf(fieldNames);
        this.query = query;
        this.createTime = createTime;
        this.executionStartTime = executionStartTime;
        this.endTime = endTime;
        this.queuedTimeMs = queuedTimeMs;
        this.analysisTimeMs = analysisTimeMs;
        this.distributedPlanningTimeMs = distributedPlanningTimeMs;
        this.totalSplitWallTimeMs = totalSplitWallTimeMs;
        this.totalSplitCpuTimeMs = totalSplitCpuTimeMs;
        this.totalBytes = totalBytes;
        this.totalRows = totalRows;
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
    public Long getQueryWallTimeMs()
    {
        if (createTime == null || endTime == null) {
            return null;
        }
        return endTime.getMillis() - createTime.getMillis();
    }

    @EventField
    public Long getQueuedTimeMs()
    {
        return queuedTimeMs;
    }

    @EventField
    public Long getAnalysisTimeMs()
    {
        return analysisTimeMs;
    }

    @EventField
    public Long getDistributedPlanningTimeMs()
    {
        return distributedPlanningTimeMs;
    }

    @EventField
    public Long getTotalSplitWallTimeMs()
    {
        return totalSplitWallTimeMs;
    }

    @EventField
    public Long getTotalSplitCpuTimeMs()
    {
        return totalSplitCpuTimeMs;
    }

    @EventField
    public Long getBytesPerSec()
    {
        Long queryWallTimeMs = getQueryWallTimeMs();
        if (totalBytes == null || queryWallTimeMs == null) {
            return null;
        }
        return totalBytes * 1000 / (queryWallTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getBytesPerCpuSec()
    {
        if (totalBytes == null || totalSplitCpuTimeMs == null) {
            return null;
        }
        return totalBytes * 1000 / (totalSplitCpuTimeMs + 1); // add 1 to avoid divide by zero

    }

    @EventField
    public Long getTotalBytes()
    {
        return totalBytes;
    }

    @EventField
    public Long getRowsPerSec()
    {
        Long queryWallTimeMs = getQueryWallTimeMs();
        if (totalRows == null || queryWallTimeMs == null) {
            return null;
        }
        return totalRows * 1000 / (queryWallTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getRowsPerCpuSec()
    {
        if (totalRows == null || totalSplitCpuTimeMs == null) {
            return null;
        }
        return totalRows * 1000 / (totalSplitCpuTimeMs + 1); // add 1 to avoid divide by zero
    }

    @EventField
    public Long getTotalRows()
    {
        return totalRows;
    }

    @EventField
    public Integer getSplits()
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
