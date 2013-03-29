package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;

import java.net.URI;

import static com.facebook.presto.execution.FailureInfo.toFailure;
import static com.google.common.base.Preconditions.checkNotNull;

public class FailedQueryExecution
        implements QueryExecution
{
    private final QueryInfo queryInfo;

    public FailedQueryExecution(String queryId, String query, Session session, URI self, Throwable cause)
    {
        QueryStats queryStats = new QueryStats();
        queryStats.recordEnd();

        queryInfo = new QueryInfo(
                checkNotNull(queryId, "queryId is null"),
                checkNotNull(session, "session is null"),
                QueryState.FAILED,
                checkNotNull(self, "self is null"),
                ImmutableList.<String>of(),
                checkNotNull(query, "query is null"),
                queryStats,
                null,
                toFailure(checkNotNull(cause, "cause is null")));
    }

    @Override
    public QueryInfo getQueryInfo()
    {
        return queryInfo;
    }

    @Override
    public void start()
    {
        // no-op
    }

    @Override
    public void updateState(boolean forceRefresh)
    {
        // no-op
    }

    @Override
    public void cancel()
    {
        // no-op
    }

    @Override
    public void fail(Throwable cause)
    {
        // no-op
    }

    @Override
    public void cancelStage(String stageId)
    {
        // no-op
    }
}
