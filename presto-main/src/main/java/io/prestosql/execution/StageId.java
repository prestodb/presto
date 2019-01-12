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
package io.prestosql.execution;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.prestosql.spi.QueryId;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StageId
{
    @JsonCreator
    public static StageId valueOf(String stageId)
    {
        List<String> ids = QueryId.parseDottedId(stageId, 2, "stageId");
        return valueOf(ids);
    }

    public static StageId valueOf(List<String> ids)
    {
        checkArgument(ids.size() == 2, "Expected two ids but got: %s", ids);
        return new StageId(new QueryId(ids.get(0)), Integer.parseInt(ids.get(1)));
    }

    private final QueryId queryId;
    private final int id;

    public StageId(String queryId, int id)
    {
        this(new QueryId(queryId), id);
    }

    public StageId(QueryId queryId, int id)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.id = id;
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public int getId()
    {
        return id;
    }

    @Override
    @JsonValue
    public String toString()
    {
        return queryId + "." + id;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, queryId);
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
        final StageId other = (StageId) obj;
        return Objects.equals(this.id, other.id) &&
                Objects.equals(this.queryId, other.queryId);
    }
}
