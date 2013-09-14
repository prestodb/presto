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

public enum StageState
{
    /**
     * Stage is planned but has not been scheduled yet.  A stage will
     * be in the planned state until, the dependencies of the stage
     * have begun producing output.
     */
    PLANNED(false),
    /**
     * Stage tasks are being scheduled on nodes.
     */
    SCHEDULING(false),
    /**
     * Stage has been scheduled on nodes and ready to execute, but all tasks are still queued.
     */
    SCHEDULED(false),
    /**
     * Stage is running.
     */
    RUNNING(false),
    /**
     * Stage has finished executing and all output has been consumed.
     */
    FINISHED(true),
    /**
     * Stage was canceled by a user.
     */
    CANCELED(true),
    /**
     * Stage execution failed.
     */
    FAILED(true);

    private final boolean doneState;

    private StageState(boolean doneState)
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

    public static Predicate<StageState> inDoneState()
    {
        return new Predicate<StageState>()
        {
            @Override
            public boolean apply(StageState state)
            {
                return state.isDone();
            }
        };
    }
}
