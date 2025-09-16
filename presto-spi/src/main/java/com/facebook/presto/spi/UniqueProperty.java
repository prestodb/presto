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

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class UniqueProperty<E>
        implements LocalProperty<E>
{
    private final E column;

    @JsonCreator
    public UniqueProperty(@JsonProperty("column") E column)
    {
        this.column = requireNonNull(column, "column is null");
    }

    @Override
    public boolean isOrderSensitive()
    {
        return false;
    }

    @JsonProperty
    public E getColumn()
    {
        return column;
    }

    public Set<E> getColumns()
    {
        return Collections.singleton(column);
    }

    @Override
    public <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator)
    {
        return translator.apply(column)
                .map(UniqueProperty::new);
    }

    @Override
    public boolean isSimplifiedBy(LocalProperty<E> known)
    {
        return known instanceof UniqueProperty && known.equals(this);
    }

    @Override
    public Optional<LocalProperty<E>> withConstants(Set<E> constants)
    {
        return Optional.of(this);
    }

    @Override
    public String toString()
    {
        return "U(" + column + ")";
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
        UniqueProperty<?> that = (UniqueProperty<?>) o;
        return Objects.equals(column, that.column);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column);
    }
}
