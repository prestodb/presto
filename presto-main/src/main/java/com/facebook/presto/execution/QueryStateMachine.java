package com.facebook.presto.execution;

import com.facebook.presto.event.query.QueryMonitor;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.List;

import static com.facebook.presto.execution.FailureInfo.toFailure;
import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class QueryStateMachine
{
    private static final Logger log = Logger.get(QueryStateMachine.class);

    private final String queryId;
    private final String query;
    private final Session session;
    private final URI self;
    private final QueryMonitor queryMonitor;
    private final QueryStats queryStats = new QueryStats();

    @GuardedBy("this")
    private QueryState queryState = QueryState.QUEUED;

    @GuardedBy("this")
    private Throwable failureCause;

    @GuardedBy("this")
    private List<String> outputFieldNames = ImmutableList.of();

    public QueryStateMachine(String queryId, String query, Session session, URI self, QueryMonitor queryMonitor)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.query = checkNotNull(query, "query is null");
        this.session = checkNotNull(session, "session is null");
        this.self = checkNotNull(self, "self is null");
        this.queryMonitor = checkNotNull(queryMonitor, "queryMonitor is null");
    }

    public String getQueryId()
    {
        return queryId;
    }

    public String getQuery()
    {
        return query;
    }

    public Session getSession()
    {
        return session;
    }

    public QueryStats getStats()
    {
        return queryStats;
    }

    public QueryInfo getQueryInfo()
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
        synchronized (this) {
            // transition to failed state, only if not already finished
            if (queryState.isDone()) {
                return false;
            }
            queryState = QueryState.FINISHED;
        }

        log.debug("Finished query %s", queryId);
        queryStats.recordEnd();
        queryMonitor.completionEvent(getQueryInfo());
        return true;
    }

    public boolean cancel()
    {
        // transition to canceled state, only if not already finished
        synchronized (this) {
            // transition to failed state, only if not already finished
            if (queryState.isDone()) {
                return false;
            }
            queryState = QueryState.CANCELED;
        }

        log.debug("Canceled query %s", queryId);
        queryStats.recordEnd();
        queryMonitor.completionEvent(getQueryInfo());
        return true;
    }

    public boolean fail(@Nullable Throwable cause)
    {
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

            queryState = QueryState.FAILED;
        }

        log.debug("Failed query %s", queryId);
        queryStats.recordEnd();
        queryMonitor.completionEvent(getQueryInfo());
        return true;
    }
}
