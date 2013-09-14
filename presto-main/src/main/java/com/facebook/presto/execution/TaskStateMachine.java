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
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import io.airlift.log.Logger;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkNotNull;

@ThreadSafe
public class TaskStateMachine
{
    private static final Logger log = Logger.get(TaskStateMachine.class);

    private final TaskId taskId;
    private final StateMachine<TaskState> taskState;
    private final LinkedBlockingQueue<Throwable> failureCauses = new LinkedBlockingQueue<>();

    public TaskStateMachine(TaskId taskId, Executor executor)
    {
        this.taskId = checkNotNull(taskId, "taskId is null");
        taskState = new StateMachine<>("task " + taskId, executor, TaskState.RUNNING);
        taskState.addStateChangeListener(new StateChangeListener<TaskState>()
        {
            @Override
            public void stateChanged(TaskState newValue)
            {
                log.debug("Task %s is %s", TaskStateMachine.this.taskId, newValue);
            }
        });
    }

    public TaskId getTaskId()
    {
        return taskId;
    }

    public TaskState getState()
    {
        return taskState.get();
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

    public void failed(Throwable cause)
    {
        failureCauses.add(cause);
        transitionToDoneState(TaskState.FAILED);
    }

    private void transitionToDoneState(TaskState doneState)
    {
        Preconditions.checkNotNull(doneState, "doneState is null");
        Preconditions.checkArgument(doneState.isDone(), "doneState %s is not a done state", doneState);

        taskState.setIf(doneState, new Predicate<TaskState>()
        {
            public boolean apply(TaskState currentState)
            {
                return !currentState.isDone();
            }
        });
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
        return Objects.toStringHelper(this)
                .add("taskId", taskId)
                .add("taskState", taskState)
                .add("failureCauses", failureCauses)
                .toString();
    }
}
