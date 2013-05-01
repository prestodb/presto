package com.facebook.presto.execution;

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.sql.analyzer.Session;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;

import static com.facebook.presto.execution.QueryState.CANCELED;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.FINISHED;
import static com.facebook.presto.execution.QueryState.inDoneState;
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

    private final StateMachine<QueryState> queryState;

    @GuardedBy("this")
    private Throwable failureCause;

    @GuardedBy("this")
    private List<String> outputFieldNames = ImmutableList.of();

    public QueryStateMachine(QueryId queryId, String query, Session session, URI self, Executor executor)
    {
        this.queryId = checkNotNull(queryId, "queryId is null");
        this.query = checkNotNull(query, "query is null");
        this.session = checkNotNull(session, "session is null");
        this.self = checkNotNull(self, "self is null");

        this.queryState = new StateMachine<>("query " + query, executor, QueryState.QUEUED);
        queryState.addStateChangeListener(new StateChangeListener<QueryState>() {
            @Override
            public void stateChanged(QueryState newValue)
            {
                log.debug("Query %s is %s", QueryStateMachine.this.queryId, newValue);
            }
        });
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
        QueryState state = queryState.get();

        // don't report failure info is query is marked as success
        FailureInfo failureInfo = null;
        if (state != FINISHED) {
            failureInfo = toFailure(failureCause);
        }
        return new QueryInfo(queryId,
                session,
                state,
                self,
                outputFieldNames,
                query,
                queryStats,
                stageInfo,
                failureInfo);
    }

    public synchronized void setOutputFieldNames(List<String> outputFieldNames)
    {
        checkNotNull(outputFieldNames, "outputFieldNames is null");
        this.outputFieldNames = ImmutableList.copyOf(outputFieldNames);
    }

    public synchronized QueryState getQueryState()
    {
        return queryState.get();
    }

    public synchronized boolean isDone()
    {
        return queryState.get().isDone();
    }

    public boolean beginPlanning()
    {
        // transition from queued to planning
        if (!queryState.compareAndSet(QueryState.QUEUED, QueryState.PLANNING)) {
            return false;
        }

        // planning has begun
        queryStats.recordAnalysisStart();
        return true;
    }

    public synchronized boolean starting()
    {
        // transition from queued or planning to starting
        return queryState.setIf(QueryState.STARTING, Predicates.in(ImmutableSet.of(QueryState.QUEUED, QueryState.PLANNING)));
    }

    public synchronized boolean running()
    {
        // transition to running if not already done
        return queryState.setIf(QueryState.RUNNING, Predicates.not(inDoneState()));
    }

    public boolean finished()
    {
        queryStats.recordEnd();
        return queryState.setIf(FINISHED, Predicates.not(inDoneState()));
    }

    public boolean cancel()
    {
        queryStats.recordEnd();
        synchronized (this) {
            if (failureCause == null) {
                failureCause = new RuntimeException("Query was canceled");
            }
        }
        return queryState.setIf(CANCELED, Predicates.not(inDoneState()));
    }

    public boolean fail(@Nullable Throwable cause)
    {
        queryStats.recordEnd();
        synchronized (this) {
            if (cause != null) {
                if (failureCause == null) {
                    failureCause = cause;
                }
                else {
                    failureCause.addSuppressed(cause);
                }
            }
        }
        return queryState.setIf(FAILED, Predicates.not(inDoneState()));
    }

    public void addStateChangeListener(StateChangeListener<QueryState> stateChangeListener)
    {
        queryState.addStateChangeListener(stateChangeListener);
    }

    public Duration waitForStateChange(QueryState currentState, Duration maxWait)
            throws InterruptedException
    {
        return queryState.waitForStateChange(currentState, maxWait);
    }
}
