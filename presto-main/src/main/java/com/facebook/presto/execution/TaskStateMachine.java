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

import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static com.facebook.presto.execution.TaskState.TERMINAL_TASK_STATES;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class TaskStateMachine
{
    private static final Logger log = Logger.get(TaskStateMachine.class);

    private final DateTime createdTime = DateTime.now();

    private final TaskId taskId;
    private final StateMachine<TaskState> taskState;
    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    public TaskStateMachine(TaskId taskId, Executor executor)
    {
        this.taskId = requireNonNull(taskId, "taskId is null");
        taskState = new StateMachine<>("task " + taskId, executor, TaskState.RUNNING, TERMINAL_TASK_STATES);
        taskState.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newState)
            {
                log.debug("Task %s is %s", TaskStateMachine.this.taskId, newState);
            }
        });
    }

    public DateTime getCreatedTime()
    {
        return createdTime;
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskState getState()
    {
        return taskState.get();
    }

    public ListenableFuture<TaskState> getStateChange(TaskState currentState)
    {
        requireNonNull(currentState, "currentState is null");
        checkArgument(!currentState.isDone(), "Current state is already done");

        ListenableFuture<TaskState> future = taskState.getStateChange(currentState);
        TaskState state = taskState.get();
        if (state.isDone()) {
            return immediateFuture(state);
        }
        return future;
    }

    public LinkedBlockingQueue<Throwable> getFailureCauses()
    {
        return failureCauses;
    }

    public void finished()
    {
        transitionToDoneState(TaskState.FINISHED);
    }

    public void cancel()
    {
        transitionToDoneState(TaskState.CANCELED);
    }

    public void abort()
    {
        transitionToDoneState(TaskState.ABORTED);
    }

    public void failed(Throwable cause)
    {
        failureCauses.add(cause);
        transitionToDoneState(TaskState.FAILED);
    }

    private void transitionToDoneState(TaskState doneState)
    {
        requireNonNull(doneState, "doneState is null");
        checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

        taskState.setIf(doneState, currentState -> !currentState.isDone());
    }

    public Duration waitForStateChange(TaskState currentState, Duration maxWait)
            throws InterruptedException
    {
        return taskState.waitForStateChange(currentState, maxWait);
    }

    public void addStateChangeListener(StateChangeListener<TaskState> stateChangeListener)
    {
        taskState.addStateChangeListener(stateChangeListener);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("taskId", taskId)
                .add("taskState", taskState)
                .add("failureCauses", failureCauses)
                .toString();
    }
}
