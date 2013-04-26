package com.facebook.presto.execution;

import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.util.Failures.toFailure;
import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger log = Logger.get(QueryStateMachine.class);

    private final QueryId queryId;
    private final String query;
    private final Session session;
    private final URI self;
    private final QueryStats queryStats = new QueryStats();

    @GuardedBy("this")
    private QueryState queryState = QueryState.QUEUED;

    @GuardedBy("this")
    private Throwable failureCause;

    @GuardedBy("this")
    private List<String> outputFieldNames = ImmutableList.of();

    @GuardedBy("this")
    private final List<Runnable> listeners = new ArrayList<>();

    public QueryStateMachine(QueryId queryId, String query, Session session, URI self)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.query = checkNotNull(query, "query is null");
        this.session = checkNotNull(session, "session is null");
        this.self = checkNotNull(self, "self is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Session getSession()
    {
        return session;
    }

    public QueryStats getStats()
    {
        return queryStats;
    }

    public QueryInfo getQueryInfoWithoutDetails()
    {
        return getQueryInfo(null);
    }

    public synchronized QueryInfo getQueryInfo(StageInfo stageInfo)
    {
        return new QueryInfo(queryId,
                session,
                queryState,
                self,
                outputFieldNames,
                query,
                queryStats,
                stageInfo,
                toFailure(failureCause));
    }

    public synchronized void setOutputFieldNames(List<String> outputFieldNames)
    {
        checkNotNull(outputFieldNames, "outputFieldNames is null");
        this.outputFieldNames = ImmutableList.copyOf(outputFieldNames);
    }

    public synchronized QueryState getQueryState()
    {
        return queryState;
    }

    public synchronized boolean isDone()
    {
        return queryState.isDone();
    }

    public boolean beginPlanning()
    {
        synchronized (this) {
            if (queryState != QueryState.QUEUED) {
                return false;
            }
            queryState = QueryState.PLANNING;
        }

        // planning has begun
        queryStats.recordAnalysisStart();
        return true;
    }

    public synchronized boolean starting()
    {
        if (queryState != QueryState.QUEUED && queryState != QueryState.PLANNING) {
            return false;
        }
        queryState = QueryState.STARTING;
        return true;
    }

    public synchronized boolean running()
    {
        if (queryState.isDone()) {
            return false;
        }
        queryState = QueryState.RUNNING;
        return true;
    }

    public boolean finished()
    {
        List<Runnable> listenersToNotify;
        synchronized (this) {
            // transition to failed state, only if not already finished
            if (queryState.isDone()) {
                return false;
            }
            queryState = QueryState.FINISHED;
            listenersToNotify = ImmutableList.copyOf(listeners);
        }

        log.debug("Finished query %s", queryId);
        queryStats.recordEnd();
        notifyListeners(listenersToNotify);
        return true;
    }

    public boolean cancel()
    {
        List<Runnable> listenersToNotify;
        // transition to canceled state, only if not already finished
        synchronized (this) {
            // transition to failed state, only if not already finished
            if (queryState.isDone()) {
                return false;
            }
            queryState = QueryState.CANCELED;
            failureCause = new RuntimeException("Query was canceled");
            listenersToNotify = ImmutableList.copyOf(listeners);
        }

        log.debug("Canceled query %s", queryId);
        queryStats.recordEnd();
        notifyListeners(listenersToNotify);
        return true;
    }

    public boolean fail(@Nullable Throwable cause)
    {
        List<Runnable> listenersToNotify;
        synchronized (this) {
            // only fail is query has not already completed successfully
            if (queryState.isDone() && (queryState != QueryState.FAILED)) {
                return false;
            }

            if (cause != null) {
                if (failureCause == null) {
                    failureCause = cause;
                }
                else {
                    failureCause.addSuppressed(cause);
                }
            }

            // skip if failure has already been reported
            if (queryState == QueryState.FAILED) {
                return true;
            }

            listenersToNotify = ImmutableList.copyOf(listeners);
            queryState = QueryState.FAILED;
        }

        log.debug("Failed query %s", queryId);
        queryStats.recordEnd();
        notifyListeners(listenersToNotify);
        return true;
    }

    public void addListener(Runnable listener)
    {
        boolean needsToNotify;
        synchronized (this) {
            listeners.add(listener);
            needsToNotify = queryState.isDone();
        }

        if (needsToNotify) {
            notifyListeners(ImmutableList.of(listener));
        }
    }

    private void notifyListeners(List<Runnable> listeners)
    {
        for (Runnable listener : listeners) {
            listener.run();
        }
    }
}
