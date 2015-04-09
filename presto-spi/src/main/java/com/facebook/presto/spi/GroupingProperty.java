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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class GroupingProperty<T>
        implements LocalProperty<T>
{
    private final Set<T> columns;

    public GroupingProperty(Collection<T> columns)
    {
        requireNonNull(columns, "columns is null");

        this.columns = Collections.unmodifiableSet(new HashSet<>(columns));
    }

    public Set<T> getColumns()
    {
        return columns;
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("G(");
        Iterator<T> columns = this.columns.iterator();
        while (columns.hasNext()) {
            builder.append(columns.next().toString());
            if (columns.hasNext()) {
                builder.append(", ");
            }
        }
        builder.append(")");

        return builder.toString();
    }

    /**
     * @return Optional.empty() if any of the columns could not be translated
     */
    @Override
    public <E> Optional<LocalProperty<E>> translate(Function<T, Optional<E>> translator)
    {
        Set<Optional<E>> translated = columns.stream()
                .map(translator)
                .collect(Collectors.toSet());

        if (translated.stream().allMatch(Optional::isPresent)) {
            Set<E> columns = translated.stream()
                    .map(Optional::get)
                    .collect(Collectors.toSet());

            return Optional.of(new GroupingProperty<>(columns));
        }

        return Optional.empty();
    }
}
