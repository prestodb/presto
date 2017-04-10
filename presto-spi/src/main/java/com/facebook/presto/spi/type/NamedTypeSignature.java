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
package com.facebook.presto.spi.type;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static java.lang.String.format;

public class NamedTypeSignature
{
    private final String name;
    private final TypeSignature typeSignature;

    @JsonCreator
    public NamedTypeSignature(
            @JsonProperty("name") String name,
            @JsonProperty("typeSignature") TypeSignature typeSignature)
    {
        this.name = name;
        this.typeSignature = typeSignature;
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
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

        NamedTypeSignature other = (NamedTypeSignature) o;

        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.typeSignature, other.typeSignature);
    }

    @Override
    public String toString()
    {
        return format("%s %s", name, typeSignature);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, typeSignature);
    }
}
