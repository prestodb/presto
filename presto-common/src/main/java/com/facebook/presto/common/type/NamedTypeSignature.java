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
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class NamedTypeSignature
{
    private final Optional<RowFieldName> fieldName;
    private final TypeSignature typeSignature;

    @JsonCreator
    public NamedTypeSignature(
            @JsonProperty("fieldName") Optional<RowFieldName> fieldName,
            @JsonProperty("typeSignature") TypeSignature typeSignature)
    {
        this.fieldName = requireNonNull(fieldName, "fieldName is null");
        this.typeSignature = requireNonNull(typeSignature, "typeSignature is null");
    }

    @JsonProperty
    public Optional<RowFieldName> getFieldName()
    {
        return fieldName;
    }

    @JsonProperty
    public TypeSignature getTypeSignature()
    {
        return typeSignature;
    }

    public Optional<String> getName()
    {
        return getFieldName().map(RowFieldName::getName);
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

        return Objects.equals(this.fieldName, other.fieldName) &&
                Objects.equals(this.typeSignature, other.typeSignature);
    }

    @Override
    public String toString()
    {
        if (fieldName.isPresent()) {
            return format("%s %s", fieldName.get(), typeSignature);
        }
        return typeSignature.toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fieldName, typeSignature);
    }
}
