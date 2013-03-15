package com.facebook.presto.execution;

import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkState;

public class QueryExecutionState
{
    private final AtomicReference<QueryState> queryState = new AtomicReference<>(QueryState.QUEUED);

    public synchronized void transitionToPlanningState()
    {
        if (!queryState.get().isDone()) {
            checkState(queryState.compareAndSet(QueryState.QUEUED, QueryState.PLANNING), "Could not transition to planning state, state is %s", queryState.get());
        }
    }

    public synchronized void transitionToStartingState()
    {
        if (!queryState.get().isDone()) {
            checkState(queryState.compareAndSet(QueryState.QUEUED, QueryState.STARTING)
                    || queryState.compareAndSet(QueryState.PLANNING, QueryState.STARTING), "Could not transition to starting state, state is %s", queryState.get());
        }
    }

    public synchronized void transitionToRunningState()
    {
        if (!queryState.get().isDone()) {
            checkState(queryState.compareAndSet(QueryState.STARTING, QueryState.RUNNING), "Could not transition to running state, state is %s", queryState.get());
        }
    }

    public synchronized boolean toState(QueryState targetState)
    {
        while (true) {
            QueryState currentState = this.queryState.get();
            if (currentState.isDone()) {
                return currentState == targetState;
            }
            if (this.queryState.compareAndSet(currentState, targetState)) {
                return true;
            }
        }
    }

    public synchronized QueryState getQueryState()
    {
        return queryState.get();
    }

    public synchronized boolean isDone()
    {
        return queryState.get().isDone();
    }

    public synchronized void checkQueryState(QueryState enforcedState)
    {
        QueryState currentState = this.queryState.get();

        checkState(currentState == enforcedState,
                "Expected query to be in state %s but was in state %s",
                enforcedState,
                currentState);
    }
}
