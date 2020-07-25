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
package com.facebook.presto.common.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class RowFieldName
{
    private final String name;
    private final boolean delimited;

    @JsonCreator
    public RowFieldName(
            @JsonProperty("name") String name,
            @JsonProperty("delimited") boolean delimited)
    {
        this.name = requireNonNull(name, "name is null");
        this.delimited = delimited;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public boolean isDelimited()
    {
        return delimited;
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

        RowFieldName other = (RowFieldName) o;

        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.delimited, other.delimited);
    }

    @Override
    public String toString()
    {
        if (!isDelimited()) {
            return name;
        }
        return '"' + name.replace("\"", "\"\"") + '"';
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, delimited);
    }
}
