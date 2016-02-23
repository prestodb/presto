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

import com.facebook.presto.OutputBuffers;
import com.facebook.presto.Session;
import com.facebook.presto.TaskSource;
import com.facebook.presto.execution.StateMachine.StateChangeListener;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.sql.planner.PlanFragment;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public interface TaskManager
{
    /**
     * Gets all of the currently tracked tasks.  This will included
     * uninitialized, running, and completed tasks.
     */
    List<TaskInfo> getAllTaskInfo();

    /**
     * Gets the info for the specified task.  If the task has not been created
     * yet, an uninitialized task is created and the info is returned.
     *
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    TaskInfo getTaskInfo(TaskId taskId);

    /**
     * Gets the status for the specified task.
     */
    TaskStatus getTaskStatus(TaskId taskId);

    /**
     * Gets future info for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the info is returned immediately.
     *
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    CompletableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState);

    /**
     * Gets the unique instance id of a task.  This can be used to detect a task
     * that was destroyed and recreated.
     */
    String getTaskInstanceId(TaskId taskId);

    /**
     * Gets future status for the task after the state changes from
     * {@code current state}. If the task has not been created yet, an
     * uninitialized task is created and the future is returned.  If the task
     * is already in a final state, the status is returned immediately.
     *
     * NOTE: this design assumes that only tasks that will eventually exist are
     * queried.
     */
    CompletableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState);

    void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments);

    /**
     * Updates the task plan, sources and output buffers.  If the task does not
     * already exist, is is created and then updated.
     */
    TaskInfo updateTask(Session session, TaskId taskId, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers);

    /**
     * Cancels a task.  If the task does not already exist, is is created and then
     * canceled.
     */
    TaskInfo cancelTask(TaskId taskId);

    /**
     * Aborts a task.  If the task does not already exist, is is created and then
     * aborted.
     */
    TaskInfo abortTask(TaskId taskId);

    /**
     * Gets results from a task either immediately or in the future.  If the
     * task or buffer has not been created yet, an uninitialized task is
     * created and a future is returned.
     *
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    CompletableFuture<BufferResult> getTaskResults(TaskId taskId, TaskId outputName, long startingSequenceId, DataSize maxSize);

    /**
     * Aborts a result buffer for a task.  If the task or buffer has not been
     * created yet, an uninitialized task is created and a the buffer is
     * aborted.
     *
     * NOTE: this design assumes that only tasks and buffers that will
     * eventually exist are queried.
     */
    TaskInfo abortTaskResults(TaskId taskId, TaskId outputId);

    /**
     * Adds a state change listener to the specified task.
     */
    void addStateChangeListener(TaskId taskId, StateChangeListener<TaskState> stateChangeListener);
}
