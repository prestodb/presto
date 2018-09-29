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
package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;

public final class KafkaColumn
{
    private final String name;
    private final Type type;
    private final String jsonPath;

    @JsonCreator
    public KafkaColumn(@JsonProperty("name") String name, @JsonProperty("type") Type type,
            @JsonProperty("jsonPath") String jsonPath)
    {
        checkArgument(!isNullOrEmpty(name), "name is null or is empty");
        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.jsonPath = (jsonPath == null || jsonPath.trim().isEmpty()) ? this.name : jsonPath;
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

    @Override
    public String toString()
    {
        return "KafkaColumn [name=" + name + ", type=" + type + ", jsonPath=" + jsonPath + "]";
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((jsonPath == null) ? 0 : jsonPath.hashCode());
        result = prime * result + ((name == null) ? 0 : name.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        KafkaColumn other = (KafkaColumn) obj;
        if (jsonPath == null) {
            if (other.jsonPath != null) {
                return false;
            }
        }
        else if (!jsonPath.equals(other.jsonPath)) {
            return false;
        }
        if (name == null) {
            if (other.name != null) {
                return false;
            }
        }
        else if (!name.equals(other.name)) {
            return false;
        }
        if (type == null) {
            if (other.type != null) {
                return false;
            }
        }
        else if (!type.equals(other.type)) {
            return false;
        }
        return true;
    }
}
