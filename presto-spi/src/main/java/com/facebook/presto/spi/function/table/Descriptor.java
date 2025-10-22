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
package com.facebook.presto.spi.function.table;

import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.spi.function.table.Preconditions.checkArgument;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class Descriptor
{
    private final List<Field> fields;

    @JsonCreator
    public Descriptor(@JsonProperty("fields") List<Field> fields)
    {
        requireNonNull(fields, "fields is null");
        checkArgument(!fields.isEmpty(), "descriptor has no fields");
        this.fields = unmodifiableList(fields);
    }

    public static Descriptor descriptor(String... names)
    {
        List<Field> fields = Arrays.stream(names)
                .map(name -> new Field(name, Optional.empty()))
                .collect(Collectors.toList());
        return new Descriptor(fields);
    }

    public static Descriptor descriptor(List<String> names, List<Type> types)
    {
        requireNonNull(names, "names is null");
        requireNonNull(types, "types is null");
        checkArgument(names.size() == types.size(), "names and types lists do not match");
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            fields.add(new Field(names.get(i), Optional.of(types.get(i))));
        }
        return new Descriptor(fields);
    }

    @JsonProperty
    public List<Field> getFields()
    {
        return fields;
    }

    public boolean isTyped()
    {
        return fields.stream().allMatch(field -> field.type.isPresent());
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
        Descriptor that = (Descriptor) o;
        return fields.equals(that.fields);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fields);
    }

    public static class Field
    {
        // Optional: allows returning anonymous columns.
        private final Optional<String> name;
        private final Optional<Type> type;

        public Field(String name, Optional<Type> type)
        {
            this(Optional.of(name), type);
        }

        @JsonCreator
        public Field(@JsonProperty("name") Optional<String> name, @JsonProperty("type") Optional<Type> type)
        {
            this.name = requireNonNull(name, "name is null");
            name.ifPresent(nameValue -> checkArgument(!nameValue.isEmpty(), "name is empty"));
            this.type = requireNonNull(type, "type is null");
        }

        @JsonProperty
        public Optional<String> getName()
        {
            return name;
        }

        @JsonProperty
        public Optional<Type> getType()
        {
            return type;
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
            Field field = (Field) o;
            return name.equals(field.name) && type.equals(field.type);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, type);
        }
    }
}
