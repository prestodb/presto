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
public class StageExecutionId
{
    private final StageId stageId;
    private final int id;

    @JsonCreator
    public static StageExecutionId valueOf(String stageExecutionAttemptId)
    {
        return valueOf(parseDottedId(stageExecutionAttemptId, 3, "stageExecutionAttemptId"));
    }

    public static StageExecutionId valueOf(List<String> ids)
    {
        checkArgument(ids.size() == 3, "Expected 3 ids but got: %s", ids);
        return new StageExecutionId(new StageId(new QueryId(ids.get(0)), parseInt(ids.get(1))), parseInt(ids.get(2)));
    }

    @ThriftConstructor
    public StageExecutionId(StageId stageId, int id)
    {
        this.stageId = requireNonNull(stageId, "stageId is null");
        this.id = id;
    }

    @ThriftField(1)
    public StageId getStageId()
    {
        return stageId;
    }

    @ThriftField(2)
    public int getId()
    {
        return id;
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
        StageExecutionId that = (StageExecutionId) o;
        return id == that.id &&
                Objects.equals(stageId, that.stageId);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(stageId, id);
    }

    @Override
    @JsonValue
    public String toString()
    {
        return stageId + "." + id;
    }
}
