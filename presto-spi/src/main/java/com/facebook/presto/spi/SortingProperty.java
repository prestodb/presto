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

import java.util.Optional;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SortingProperty<T>
    implements LocalProperty<T>
{
    private final T column;
    private final SortOrder order;

    public SortingProperty(T column, SortOrder order)
    {
        requireNonNull(column, "column is null");
        requireNonNull(order, "order is null");

        this.column = column;
        this.order = order;
    }

    public T getColumn()
    {
        return column;
    }

    public SortOrder getOrder()
    {
        return order;
    }

    @Override
    public String toString()
    {
        String ordering = "";
        switch (order) {
            case ASC_NULLS_FIRST:
            case ASC_NULLS_LAST:
                ordering = "\u2191";
                break;
            case DESC_NULLS_FIRST:
            case DESC_NULLS_LAST:
                ordering = "\u2193";
                break;
        }

        return "S" + ordering + "(" + column + ")";
    }

    /**
     * Returns Optional.empty() if the column could not be translated
     */
    @Override
    public <E> Optional<LocalProperty<E>> translate(Function<T, Optional<E>> translator)
    {
        Optional<E> translated = translator.apply(column);

        if (translated.isPresent()) {
            return Optional.of(new SortingProperty<>(translated.get(), order));
        }

        return Optional.empty();
    }
}
