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
package com.facebook.presto.spi.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PrestoPrincipal
{
    private final PrincipalType type;
    private final String name;

    @JsonCreator
    public PrestoPrincipal(@JsonProperty("type") PrincipalType type, @JsonProperty("name") String name)
    {
        this.type = requireNonNull(type, "type is null");
        this.name = requireNonNull(name, "name is null").toLowerCase(ENGLISH);
    }

    @JsonProperty
    public PrincipalType getType()
    {
        return type;
    }

    @JsonProperty
    public String getName()
    {
        return name;
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
        PrestoPrincipal prestoPrincipal = (PrestoPrincipal) o;
        return type == prestoPrincipal.type &&
                Objects.equals(name, prestoPrincipal.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, name);
    }

    @Override
    public String toString()
    {
        return type + " " + name;
    }
}
