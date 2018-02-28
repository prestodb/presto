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
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class NestedColumn
{
    private final List<String> names;

    @JsonCreator
    public NestedColumn(@JsonProperty("names") List<String> names)
    {
        this.names = requireNonNull(names);
    }

    @JsonProperty
    public List<String> getNames()
    {
        return names;
    }

    public String getBase()
    {
        return names.get(0);
    }

    public List<String> getRest()
    {
        // TODO assert size > 1;
        return names.subList(1, names.size());
    }

    public String getName()
    {
        return names.stream().collect(Collectors.joining("."));
    }

    @JsonProperty
    public Type getType()
    {
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(names);
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

        NestedColumn other = (NestedColumn) obj;
        return Objects.equals(this.names, other.names);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("NestedColumns{");
        sb.append("name='").append(getName()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
