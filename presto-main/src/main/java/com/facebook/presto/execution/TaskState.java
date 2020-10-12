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

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;

import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableSet.toImmutableSet;

@ThriftEnum
public enum TaskState
{
    /**
     * Task is planned but has not been scheduled yet.  A task will
     * be in the planned state until, the dependencies of the task
     * have begun producing output.
     */
    PLANNED(false, 0),
    /**
     * Task is running.
     */
    RUNNING(false, 1),
    /**
     * Task has finished executing and all output has been consumed.
     */
    FINISHED(true, 2),
    /**
     * Task was canceled by a user.
     */
    CANCELED(true, 3),
    /**
     * Task was aborted due to a failure in the query.  The failure
     * was not in this task.
     */
    ABORTED(true, 4),
    /**
     * Task execution failed.
     */
    FAILED(true, 5);

    public static final Set<TaskState> TERMINAL_TASK_STATES = Stream.of(TaskState.values()).filter(TaskState::isDone).collect(toImmutableSet());

    private final boolean doneState;
    private final int value;

    TaskState(boolean doneState, int value)
    {
        this.doneState = doneState;
        this.value = value;
    }

    /**
     * Is this a terminal state.
     */
    public boolean isDone()
    {
        return doneState;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }
}
