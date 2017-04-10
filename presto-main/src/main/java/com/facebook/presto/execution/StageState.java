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

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public enum StageState
{
    /**
     * Stage is planned but has not been scheduled yet.  A stage will
     * be in the planned state until, the dependencies of the stage
     * have begun producing output.
     */
    PLANNED(false, false),
    /**
     * Stage tasks are being scheduled on nodes.
     */
    SCHEDULING(false, false),
    /**
     * All stage tasks have been scheduled, but splits are still being scheduled.
     */
    SCHEDULING_SPLITS(false, false),
    /**
     * Stage has been scheduled on nodes and ready to execute, but all tasks are still queued.
     */
    SCHEDULED(false, false),
    /**
     * Stage is running.
     */
    RUNNING(false, false),
    /**
     * Stage has finished executing and all output has been consumed.
     */
    FINISHED(true, false),
    /**
     * Stage was canceled by a user.
     */
    CANCELED(true, false),
    /**
     * Stage was aborted due to a failure in the query.  The failure
     * was not in this stage.
     */
    ABORTED(true, true),
    /**
     * Stage execution failed.
     */
    FAILED(true, true);

    public static final Set<StageState> TERMINAL_STAGE_STATES = Stream.of(StageState.values()).filter(StageState::isDone).collect(toImmutableSet());

    private final boolean doneState;
    private final boolean failureState;

    StageState(boolean doneState, boolean failureState)
    {
        checkArgument(!failureState || doneState, "%s is a non-done failure state", name());
        this.doneState = doneState;
        this.failureState = failureState;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    /**
     * Is this a non-success terminal state.
     */
    public boolean isFailure()
    {
        return failureState;
    }
}
