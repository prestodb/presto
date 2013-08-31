package com.facebook.presto.execution;

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.sql.analyzer.Session;
import io.airlift.units.Duration;

import java.net.URI;
import java.util.concurrent.Executor;

public class FailedQueryExecution
        implements QueryExecution
{
    private final QueryInfo queryInfo;

    public FailedQueryExecution(QueryId queryId, String query, Session session, URI self, Executor executor, Throwable cause)
    {
        QueryStateMachine queryStateMachine = new QueryStateMachine(queryId, query, session, self, executor);
        queryStateMachine.fail(cause);

        queryInfo = queryStateMachine.getQueryInfo(null);
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
    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return maxWait;
    }

    @Override
    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        stateChangeListener.stateChanged(QueryState.FAILED);
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
    public void cancelStage(StageId stageId)
    {
        // no-op
    }

    @Override
    public void recordHeartbeat()
    {
        // no-op
    }
}
