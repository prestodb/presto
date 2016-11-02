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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Tuple
        implements Comparable<Tuple>
{
    private final List<Type> types;
    private final List<Optional<Object>> values;

    public static Tuple of(List<Type> types, Object... values)
    {
        return new Tuple(types, ImmutableList.copyOf(values).stream()
                .map(Optional::ofNullable)
                .collect(Collectors.toList()));
    }

    public Tuple(Type type, Optional<Object> value)
    {
        this(ImmutableList.of(type), ImmutableList.of(requireNonNull(value, "value is null")));
    }

    public Tuple(List<Type> types, List<Optional<Object>> values)
    {
        this.types = requireNonNull(types, "types is null");
        this.values = requireNonNull(values, "values is null");

        checkArgument(!types.isEmpty(), "types is empty");
        checkArgument(types.size() == values.size(), "types and values must have the same number of elements");
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public List<Optional<Object>> getValues()
    {
        return values;
    }

    @Override
    public int compareTo(Tuple o)
    {
        checkArgument(o.getTypes().size() == types.size(), "types must be of same size");
        for (int i = 0; i < types.size(); i++) {
            checkArgument(o.getTypes().get(i).equals(types.get(i)), "types must be the same");
        }

        for (int i = 0; i < types.size(); i++) {
            Optional<Object> o1 = values.get(i);
            Optional<Object> o2 = o.getValues().get(i);

            // This is stricter than it needs to be, because we can compare a null with a null.
            // However, we skip such shards during organization, and therefore should never be able to get to this.
            checkArgument(o1.isPresent() && o2.isPresent(), "cannot compare tuples with empty values");

            int result = compare(o1.get(), o2.get());
            if (result == 0) {
                continue;
            }

            return result;
        }
        return 0;
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
        Tuple tuple = (Tuple) o;
        return Objects.equals(types, tuple.types) &&
                Objects.equals(values, tuple.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(types, values);
    }

    @SuppressWarnings("unchecked")
    private static <T> int compare(Object o1, Object o2)
    {
        return ((Comparable<T>) o1).compareTo((T) o2);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("values", values)
                .toString();
    }
}
