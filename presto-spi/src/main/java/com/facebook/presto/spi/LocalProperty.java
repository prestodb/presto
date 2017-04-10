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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = ConstantProperty.class, name = "constant"),
        @JsonSubTypes.Type(value = SortingProperty.class, name = "sorting"),
        @JsonSubTypes.Type(value = GroupingProperty.class, name = "grouping"),
})
public interface LocalProperty<E>
{
    <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator);

    /**
     * Return true if the actual LocalProperty can be used to simplify this LocalProperty
     */
    boolean isSimplifiedBy(LocalProperty<E> actual);

    /**
     * Simplfies this LocalProperty provided that the specified inputs are constants
     */
    default Optional<LocalProperty<E>> withConstants(Set<E> constants)
    {
        Set<E> set = new HashSet<>(getColumns());
        set.removeAll(constants);

        if (set.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(constrain(set));
    }

    /**
     * Return a new instance with the give (reduced) set of columns
     */
    default LocalProperty<E> constrain(Set<E> columns)
    {
        if (!columns.equals(getColumns())) {
            throw new IllegalArgumentException(String.format("Cannot constrain %s with %s", this, columns));
        }
        return this;
    }

    Set<E> getColumns();
}
