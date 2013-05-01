/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Predicate;

public enum QueryState
{
    /**
     * Query has been accepted and is awaiting execution.
     */
    QUEUED(false),
    /**
     * Query is being planned.
     */
    PLANNING(false),
    /**
     * Query execution is being started.
     */
    STARTING(false),
    /**
     * Query has at least one task in the output stage.
     */
    RUNNING(false),
    /**
     * Query has finished executing and all output has been consumed.
     */
    FINISHED(true),
    /**
     * Query was canceled by a user.
     */
    CANCELED(true),
    /**
     * Query execution failed.
     */
    FAILED(true);

    private final boolean doneState;

    private QueryState(boolean doneState)
    {
        this.doneState = doneState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    public static Predicate<QueryState> inDoneState()
    {
        return new Predicate<QueryState>()
        {
            @Override
            public boolean apply(QueryState state)
            {
                return state.isDone();
            }
        };
    }
}
