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
package com.facebook.presto.tvf;

import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class NativeDescriptor
{
    private final List<NativeField> fields;

    @JsonCreator
    public NativeDescriptor(@JsonProperty("fields") List<NativeField> fields)
    {
        requireNonNull(fields, "fields is null");
        checkArgument(!fields.isEmpty(), "descriptor has no fields");
        this.fields = unmodifiableList(fields);
    }

    @JsonProperty
    public List<NativeField> getFields()
    {
        return fields;
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
        NativeDescriptor that = (NativeDescriptor) o;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
    }

    public static class NativeField
    {
        private final Optional<String> name;
        private final Optional<TypeSignature> typeSignature;

        @JsonCreator
        public NativeField(
                @JsonProperty("name") Optional<String> name,
                @JsonProperty("typeSignature") Optional<TypeSignature> typeSignature)
        {
            this.name = requireNonNull(name, "name is null");
            name.ifPresent(nameValue -> checkArgument(!nameValue.isEmpty(), "name is empty"));
            this.typeSignature = requireNonNull(typeSignature, "typeSignature is null");
        }

        @JsonProperty
        public Optional<String> getName()
        {
            return name;
        }

        @JsonProperty
        public Optional<TypeSignature> getTypeSignature()
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
            NativeField field = (NativeField) o;
            return name.equals(field.name) && typeSignature.equals(field.typeSignature);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, typeSignature);
        }
    }
}
