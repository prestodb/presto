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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.operator.HostShuttingDownException;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.UNRECOVERABLE_HOST_SHUTTING_DOWN;

public class TaskShutdownManager
        implements TaskShutDownListener
{
    private final TaskStateMachine taskStateMachine;
    private final TaskContext taskContext;

    public TaskShutdownManager(TaskStateMachine taskStateMachine, TaskContext taskContext)
    {
        this.taskStateMachine = taskStateMachine;
        this.taskContext = taskContext;
    }

    @Override
    public void handleShutdown(TaskId taskId)
    {
        String errorMessage = String.format("killing pending task %s due to host being shutting down", taskId);
        taskStateMachine.gracefulShutdown(new HostShuttingDownException(errorMessage, System.nanoTime()));
    }

    @Override
    public void forceFailure(TaskId taskId)
    {
        String errorMessage = String.format("the shutdown process force the task to fail due to unable to recover", taskId);
        taskStateMachine.failed(new PrestoException(UNRECOVERABLE_HOST_SHUTTING_DOWN, errorMessage));
    }

    @Override
    public boolean isTaskDone()
    {
        return taskStateMachine.getState().isDone();
    }
}
