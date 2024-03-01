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

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class ElasticsearchColumn
{
    private final String name;
    private final Type type;
    private final String jsonPath;
    private final String jsonType;
    private final int ordinalPosition;

    @JsonCreator
    public ElasticsearchColumn(
            @JsonProperty("name") String name,
            @JsonProperty("type") Type type,
            @JsonProperty("jsonPath") String jsonPath,
            @JsonProperty("jsonType") String jsonType,
            @JsonProperty("ordinalPosition") int ordinalPosition)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.jsonPath = requireNonNull(jsonPath, "jsonPath is null");
        this.jsonType = requireNonNull(jsonType, "jsonType is null");
        this.ordinalPosition = ordinalPosition;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public String getJsonPath()
    {
        return jsonPath;
    }

    @JsonProperty
    public String getJsonType()
    {
        return jsonType;
    }

    @JsonProperty
    public int getOrdinalPosition()
    {
        return ordinalPosition;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, jsonPath, jsonType, ordinalPosition);
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

        ElasticsearchColumn other = (ElasticsearchColumn) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.jsonPath, other.jsonPath) &&
                Objects.equals(this.jsonType, other.jsonType) &&
                Objects.equals(this.ordinalPosition, other.ordinalPosition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("name", name)
                .add("type", type)
                .add("jsonPath", jsonPath)
                .add("jsonType", jsonType)
                .add("ordinalPosition", ordinalPosition)
                .toString();
    }
}
