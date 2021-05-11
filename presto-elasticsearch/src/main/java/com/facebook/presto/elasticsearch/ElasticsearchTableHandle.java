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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ConnectorTableHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchTableHandle
        implements ConnectorTableHandle
{
    public enum Type
    {
        SCAN,
        QUERY
    }

    private final Type type;
    private final String schema;
    private final String index;
    private final Optional<String> query;

    @JsonCreator
    public ElasticsearchTableHandle(
            @JsonProperty("type") Type type,
            @JsonProperty("schema") String schema,
            @JsonProperty("index") String index,
            @JsonProperty("query") Optional<String> query)
    {
        this.type = requireNonNull(type, "type is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.index = requireNonNull(index, "index is null");
        this.query = requireNonNull(query, "query is null");
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public String getSchema()
    {
        return schema;
    }

    @JsonProperty
    public Optional<String> getQuery()
    {
        return query;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, schema, index, query);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        ElasticsearchTableHandle other = (ElasticsearchTableHandle) obj;
        return Objects.equals(this.type, other.getType()) &&
                Objects.equals(this.getSchema(), other.getSchema()) &&
                Objects.equals(this.getIndex(), other.getIndex()) &&
                Objects.equals(this.getQuery(), other.getQuery());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("type", getType())
                .add("schema", getSchema())
                .add("index", getIndex())
                .add("query", getQuery())
                .toString();
    }
}
