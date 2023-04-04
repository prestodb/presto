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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.QueryId.parseDottedId;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public class TaskExecutionId
{
    private final TaskId taskId;
    private final int executionId;

    @JsonCreator
    public static TaskExecutionId valueOf(String taskExecutionId)
    {
        List<String> parts = parseDottedId(taskExecutionId, 5, "taskId");
        return new TaskExecutionId(parts.get(0), parseInt(parts.get(1)), parseInt(parts.get(2)), parseInt(parts.get(3)), parseInt(parts.get(4)));
    }

    public TaskExecutionId(String queryId, int stageId, int stageExecutionId, int taskId, int executionId)
    {
        this(new TaskId(new StageExecutionId(new StageId(new QueryId(queryId), stageId), stageExecutionId), taskId), executionId);
    }

    @ThriftConstructor
    public TaskExecutionId(TaskId taskId, int executionId)
    {
        this.taskId = requireNonNull(taskId, "taskId");
        checkArgument(executionId >= 0, "id is negative");
        this.executionId = executionId;
    }

    @ThriftField(1)
    public TaskId getTaskId()
    {
        return taskId;
    }

    @ThriftField(2)
    public int getExecutionId()
    {
        return executionId;
    }

    public QueryId getQueryId()
    {
        return taskId.getStageExecutionId().getStageId().getQueryId();
    }

    @Override
    @JsonValue
    public String toString()
    {
        return taskId + "." + executionId;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TaskExecutionId taskExecutionId = (TaskExecutionId) o;
        return executionId == taskExecutionId.executionId &&
                Objects.equals(taskId, taskExecutionId.taskId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(taskId, executionId);
    }
}
