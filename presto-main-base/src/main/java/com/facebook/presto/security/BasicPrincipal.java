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

package com.facebook.presto.security;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.security.Principal;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class BasicPrincipal
        implements Principal
{
    private final String name;

    @JsonCreator
    public BasicPrincipal(@JsonProperty("name") String name)
    {
        this.name = requireNonNull(name, "name is null");
    }

    @JsonProperty
    public String getName()
    {
        return this.name;
    }

    public String toString()
    {
        return this.name;
    }

    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        else if (o != null && this.getClass() == o.getClass()) {
            BasicPrincipal that = (BasicPrincipal) o;
            return Objects.equals(this.name, that.name);
        }
        else {
            return false;
        }
    }

    public int hashCode()
    {
        return Objects.hash(this.name);
    }
}
