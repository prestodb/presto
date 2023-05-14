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
public class TaskId
{
    private final StageExecutionId stageExecutionId;
    private final int id;
    private final int attemptNumber;

    @JsonCreator
    public static TaskId valueOf(String taskId)
    {
        List<String> parts = parseDottedId(taskId, 5, "taskId");
        return new TaskId(parts.get(0), parseInt(parts.get(1)), parseInt(parts.get(2)), parseInt(parts.get(3)), parseInt(parts.get(4)));
    }

    public TaskId(String queryId, int stageId, int stageExecutionId, int id, int attemptNumber)
    {
        this(new StageExecutionId(new StageId(new QueryId(queryId), stageId), stageExecutionId), id, attemptNumber);
    }

    @ThriftConstructor
    public TaskId(StageExecutionId stageExecutionId, int id, int attemptNumber)
    {
        this.stageExecutionId = requireNonNull(stageExecutionId, "stageExecutionId");
        this.attemptNumber = attemptNumber;
        checkArgument(id >= 0, "id is negative");
        this.id = id;
    }

    @ThriftField(1)
    public StageExecutionId getStageExecutionId()
    {
        return stageExecutionId;
    }

    @ThriftField(2)
    public int getId()
    {
        return id;
    }

    @ThriftField(3)
    public int getAttemptNumber()
    {
        return attemptNumber;
    }

    public QueryId getQueryId()
    {
        return stageExecutionId.getStageId().getQueryId();
    }

    @Override
    @JsonValue
    public String toString()
    {
        return stageExecutionId + "." + id + "." + attemptNumber;
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
        TaskId taskId = (TaskId) o;
        return attemptNumber == taskId.attemptNumber &&
                id == taskId.id &&
                Objects.equals(stageExecutionId, taskId.stageExecutionId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stageExecutionId, id, attemptNumber);
    }
}
