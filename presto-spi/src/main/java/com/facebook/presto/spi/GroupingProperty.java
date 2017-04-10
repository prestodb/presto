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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public final class GroupingProperty<E>
        implements LocalProperty<E>
{
    private final Set<E> columns;

    @JsonCreator
    public GroupingProperty(@JsonProperty("columns") Collection<E> columns)
    {
        requireNonNull(columns, "columns is null");

        this.columns = Collections.unmodifiableSet(new HashSet<>(columns));
    }

    @JsonProperty
    public Set<E> getColumns()
    {
        return columns;
    }

    @Override
    public LocalProperty<E> constrain(Set<E> columns)
    {
        if (!this.columns.containsAll(columns)) {
            throw new IllegalArgumentException(String.format("Cannot constrain %s with %s", this, columns));
        }

        return new GroupingProperty<>(columns);
    }

    @Override
    public boolean isSimplifiedBy(LocalProperty<E> known)
    {
        return known instanceof ConstantProperty || getColumns().containsAll(known.getColumns());
    }

    /**
     * @return Optional.empty() if any of the columns could not be translated
     */
    @Override
    public <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator)
    {
        Set<Optional<T>> translated = columns.stream()
                .map(translator)
                .collect(Collectors.toSet());

        if (translated.stream().allMatch(Optional::isPresent)) {
            Set<T> columns = translated.stream()
                    .map(Optional::get)
                    .collect(Collectors.toSet());

            return Optional.of(new GroupingProperty<>(columns));
        }

        return Optional.empty();
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("G(");
        builder.append(columns.stream()
                .map(Object::toString)
                .collect(Collectors.joining(", ")));
        builder.append(")");
        return builder.toString();
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
        GroupingProperty<?> that = (GroupingProperty<?>) o;
        return Objects.equals(columns, that.columns);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(columns);
    }
}
