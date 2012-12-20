package com.facebook.presto.event.query;

import com.facebook.presto.execution.QueryState;
import io.airlift.event.client.EventType;
import org.joda.time.DateTime;

import javax.annotation.concurrent.Immutable;
import java.net.URI;
import java.util.List;

@Immutable
@EventType("QueryCompletion")
public class QueryCompletionEvent
        extends AbstractQueryEvent
{
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
        super(
                queryId,
                queryState,
                uri,
                fieldNames,
                query,
                createTime,
                executionStartTime,
                endTime,
                queuedTimeMs,
                analysisTimeMs,
                distributedPlanningTimeMs,
                splits,
                outputStageJson,
                failuresJson
        );
    }
}
