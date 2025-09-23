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
package com.facebook.plugin.arrow.testingConnector;

import com.facebook.plugin.arrow.ArrowColumnHandle;
import com.facebook.plugin.arrow.ArrowTableHandle;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public class QueryArrowTableHandle
        extends ArrowTableHandle
{
    private final String query;
    private final List<ArrowColumnHandle> columns;

    public QueryArrowTableHandle(
            @JsonProperty String query,
            @JsonProperty List<ArrowColumnHandle> columns)
    {
        super("schema-" + UUID.randomUUID(), "table-" + UUID.randomUUID());
        this.query = query;
        this.columns = Collections.unmodifiableList(columns);
    }

    @JsonProperty("query")
    public String getQuery()
    {
        return query;
    }

    @JsonProperty("columns")
    public List<ArrowColumnHandle> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        return query + ":" + columns;
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
        QueryArrowTableHandle that = (QueryArrowTableHandle) o;
        return Objects.equals(getSchema(), that.getSchema()) && Objects.equals(getTable(), that.getTable()) &&
                Objects.equals(getQuery(), that.getQuery()) && Objects.equals(getColumns(), that.getColumns());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSchema(), getTable(), query, columns);
    }
}
