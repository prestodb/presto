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
package com.facebook.presto.spi;

import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class NestedField
{
    private final List<String> fields;

    @JsonCreator
    public NestedField(@JsonProperty("fields") List<String> fields)
    {
        this.fields = requireNonNull(fields);
    }

    @JsonProperty
    public List<String> getFields()
    {
        return fields;
    }

    public String getBase()
    {
        return fields.get(0);
    }

    public List<String> getRemaining()
    {
        if (fields.size() <= 1) {
            throw new IllegalArgumentException("NestedField has more than 1 field");
        }
        return fields.subList(1, fields.size());
    }

    public String getName()
    {
        return fields.stream().collect(joining("."));
    }

    @JsonProperty
    public Type getType()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
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

        NestedField other = (NestedField) obj;
        return Objects.equals(this.fields, other.fields);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("NestedFields<");
        sb.append("name='").append(getName()).append('>');
        return sb.toString();
    }
}
