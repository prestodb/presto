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

/**
 * Native descriptor for table function return type.
 */
public final class NativeDescriptor
{
    private final List<NativeField> fields;

    /**
     * Constructs a native descriptor.
     *
     * @param fields the list of fields
     */
    @JsonCreator
    public NativeDescriptor(
            @JsonProperty("fields") final List<NativeField> fields)
    {
        requireNonNull(fields, "fields is null");
        checkArgument(!fields.isEmpty(), "descriptor has no fields");
        this.fields = unmodifiableList(fields);
    }

    /**
     * Gets the fields.
     *
     * @return the list of fields
     */
    @JsonProperty
    public List<NativeField> getFields()
    {
        return fields;
    }

    /**
     * Checks equality.
     *
     * @param o the object to compare
     * @return true if equal
     */
    @Override
    public boolean equals(final Object o)
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

    /**
     * Computes hash code.
     *
     * @return the hash code
     */
    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
    }

    /**
     * Represents a field in the native descriptor.
     */
    public static final class NativeField
    {
        private final Optional<String> name;
        private final Optional<TypeSignature> typeSignature;

        /**
         * Constructs a native field.
         *
         * @param name the field name (can be empty for anonymous fields)
         * @param typeSignature the type signature
         */
        @JsonCreator
        public NativeField(
                @JsonProperty("name") final Optional<String> name,
                @JsonProperty("typeSignature")
                final Optional<TypeSignature> typeSignature)
        {
            this.name = requireNonNull(name, "name is null");
            this.typeSignature = requireNonNull(typeSignature,
                    "typeSignature is null");
        }

        /**
         * Gets the field name.
         *
         * @return the field name
         */
        @JsonProperty
        public Optional<String> getName()
        {
            return name;
        }

        /**
         * Gets the type signature.
         *
         * @return the type signature
         */
        @JsonProperty
        public Optional<TypeSignature> getTypeSignature()
        {
            return typeSignature;
        }

        /**
         * Checks equality.
         *
         * @param o the object to compare
         * @return true if equal
         */
        @Override
        public boolean equals(final Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            NativeField field = (NativeField) o;
            return name.equals(field.name)
                    && typeSignature.equals(field.typeSignature);
        }

        /**
         * Computes hash code.
         *
         * @return the hash code
         */
        @Override
        public int hashCode()
        {
            return Objects.hash(name, typeSignature);
        }
    }
}
