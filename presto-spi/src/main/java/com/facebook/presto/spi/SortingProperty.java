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

import com.facebook.presto.spi.block.SortOrder;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class SortingProperty<E>
        implements LocalProperty<E>
{
    private final E column;
    private final SortOrder order;

    @JsonCreator
    public SortingProperty(
            @JsonProperty("column") E column,
            @JsonProperty("order") SortOrder order)
    {
        requireNonNull(column, "column is null");
        requireNonNull(order, "order is null");

        this.column = column;
        this.order = order;
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

    @JsonProperty
    public SortOrder getOrder()
    {
        return order;
    }

    /**
     * Returns Optional.empty() if the column could not be translated
     */
    @Override
    public <T> Optional<LocalProperty<T>> translate(Function<E, Optional<T>> translator)
    {
        Optional<T> translated = translator.apply(column);

        if (translated.isPresent()) {
            return Optional.of(new SortingProperty<>(translated.get(), order));
        }

        return Optional.empty();
    }

    @Override
    public boolean isSimplifiedBy(LocalProperty<E> known)
    {
        return known instanceof ConstantProperty || known.equals(this);
    }

    @Override
    public String toString()
    {
        String ordering = "";
        String nullOrdering = "";
        switch (order) {
            case ASC_NULLS_FIRST:
                ordering = "\u2191";
                nullOrdering = "\u2190";
                break;
            case ASC_NULLS_LAST:
                ordering = "\u2191";
                nullOrdering = "\u2190";
                break;
            case DESC_NULLS_FIRST:
                ordering = "\u2193";
                nullOrdering = "\u2192";
                break;
            case DESC_NULLS_LAST:
                ordering = "\u2193";
                nullOrdering = "\u2192";
                break;
        }

        return "S" + ordering + nullOrdering + "(" + column + ")";
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
        SortingProperty<?> that = (SortingProperty<?>) o;
        return Objects.equals(column, that.column) &&
                Objects.equals(order, that.order);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(column, order);
    }
}
