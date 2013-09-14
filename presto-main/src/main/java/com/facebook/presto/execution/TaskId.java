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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Objects;

import java.util.List;

import static com.facebook.presto.execution.QueryId.validateId;
import static com.google.common.base.Preconditions.checkNotNull;

public class TaskId
{
    @JsonCreator
    public static TaskId valueOf(String taskId)
    {
        List<String> ids = QueryId.parseDottedId(taskId, 3, "taskId");
        return new TaskId(new StageId(new QueryId(ids.get(0)), ids.get(1)), ids.get(2));
    }

    private final StageId stageId;
    private final String id;

    public TaskId(String queryId, String stageId, String id)
    {
        this.stageId = new StageId(queryId, stageId);
        this.id = validateId(id);
    }

    public TaskId(StageId stageId, String id)
    {
        this.stageId = checkNotNull(stageId, "stageId is null");
        this.id = validateId(id);
    }

    public QueryId getQueryId()
    {
        return stageId.getQueryId();
    }

    public StageId getStageId()
    {
        return stageId;
    }

    public String getId()
    {
        return id;
    }

    @JsonValue
    public String toString()
    {
        return stageId + "." + id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(stageId, id);
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
        final TaskId other = (TaskId) obj;
        return Objects.equal(this.stageId, other.stageId) &&
                Objects.equal(this.id, other.id);
    }
}
