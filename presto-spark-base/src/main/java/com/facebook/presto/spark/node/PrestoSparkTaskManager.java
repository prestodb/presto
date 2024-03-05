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
package com.facebook.presto.spark.node;

import com.facebook.presto.Session;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskInfo;
import com.facebook.presto.execution.TaskManager;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.execution.TaskState;
import com.facebook.presto.execution.TaskStatus;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.memory.MemoryPoolAssignmentsRequest;
import com.facebook.presto.metadata.MetadataUpdates;
import com.facebook.presto.sql.planner.PlanFragment;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;

import java.util.List;
import java.util.Optional;

public class PrestoSparkTaskManager
        implements TaskManager
{
    @Override
    public List<TaskInfo> getAllTaskInfo()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo getTaskInfo(TaskId taskId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskStatus getTaskStatus(TaskId taskId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<TaskInfo> getTaskInfo(TaskId taskId, TaskState currentState)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getTaskInstanceId(TaskId taskId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<TaskStatus> getTaskStatus(TaskId taskId, TaskState currentState)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateMemoryPoolAssignments(MemoryPoolAssignmentsRequest assignments)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo updateTask(Session session, TaskId taskId, Optional<PlanFragment> fragment, List<TaskSource> sources, OutputBuffers outputBuffers, Optional<TableWriteInfo> tableWriteInfo)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo cancelTask(TaskId taskId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo abortTask(TaskId taskId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<BufferResult> getTaskResults(TaskId taskId, OutputBuffers.OutputBufferId bufferId, long startingSequenceId, DataSize maxSize)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void acknowledgeTaskResults(TaskId taskId, OutputBuffers.OutputBufferId bufferId, long sequenceId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public TaskInfo abortTaskResults(TaskId taskId, OutputBuffers.OutputBufferId bufferId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addStateChangeListener(TaskId taskId, StateMachine.StateChangeListener<TaskState> stateChangeListener)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void removeRemoteSource(TaskId taskId, TaskId remoteSourceTaskId)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void updateMetadataResults(TaskId taskId, MetadataUpdates metadataUpdates)
    {
        throw new UnsupportedOperationException();
    }
}
