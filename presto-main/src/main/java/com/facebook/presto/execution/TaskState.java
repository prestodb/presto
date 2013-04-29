/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.execution;

import com.google.common.base.Predicate;

public enum TaskState
{
    /**
     * Task is planned but has not been scheduled yet.  A task will
     * be in the planned state until, the dependencies of the task
     * have begun producing output.
     */
    PLANNED(false),
    /**
     * Task is running.
     */
    RUNNING(false),
    /**
     * Task has finished executing and all output has been consumed.
     */
    FINISHED(true),
    /**
     * Task was canceled by a user.
     */
    CANCELED(true),
    /**
     * Task execution failed.
     */
    FAILED(true);

    private final boolean doneState;

    private TaskState(boolean doneState)
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

    public static Predicate<TaskState> inDoneState()
    {
        return new Predicate<TaskState>()
        {
            @Override
            public boolean apply(TaskState state)
            {
                return state.isDone();
            }
        };
    }
}
