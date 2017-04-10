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

import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.parseInt;

public class TaskId
{
    @JsonCreator
    public static TaskId valueOf(String taskId)
    {
        return new TaskId(taskId);
    }

    private final String fullId;

    public TaskId(String queryId, int stageId, int id)
    {
        checkArgument(id >= 0, "id is negative");
        this.fullId = queryId + "." + stageId + "." + id;
    }

    public TaskId(StageId stageId, int id)
    {
        checkArgument(id >= 0, "id is negative");
        this.fullId = stageId.getQueryId().getId() + "." + stageId.getId() + "." + id;
    }

    public TaskId(String fullId)
    {
        this.fullId = fullId;
    }

    public QueryId getQueryId()
    {
        return new QueryId(QueryId.parseDottedId(fullId, 3, "taskId").get(0));
    }

    public StageId getStageId()
    {
        List<String> ids = QueryId.parseDottedId(fullId, 3, "taskId");
        return StageId.valueOf(ids.subList(0, 2));
    }

    public int getId()
    {
        return parseInt(QueryId.parseDottedId(fullId, 3, "taskId").get(2));
    }

    @Override
    @JsonValue
    public String toString()
    {
        return fullId;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fullId);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TaskId other = (TaskId) obj;
        return Objects.equals(this.fullId, other.fullId);
    }
}
