package com.facebook.presto.dispatcher;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryExecution;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryManagerStats;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

public class NoopQueryManager
        implements QueryManager
{
    @Override
    public List<BasicQueryInfo> getQueries()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addOutputInfoListener(QueryId queryId, Consumer<QueryExecution.QueryOutputInfo> listener)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addStateChangeListener(QueryId queryId, StateMachine.StateChangeListener<QueryState> listener)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<QueryState> getStateChange(QueryId queryId, QueryState currentState)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public BasicQueryInfo getQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Session getQuerySession(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isQuerySlugValid(QueryId queryId, String slug)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryState getQueryState(QueryId queryId)
            throws NoSuchElementException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void recordHeartbeat(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createQuery(QueryExecution execution)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void failQuery(QueryId queryId, Throwable cause)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelQuery(QueryId queryId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void cancelStage(StageId stageId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryManagerStats getStats()
    {
        throw new UnsupportedOperationException();
    }
}
