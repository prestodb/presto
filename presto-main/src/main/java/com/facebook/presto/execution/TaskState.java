/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
