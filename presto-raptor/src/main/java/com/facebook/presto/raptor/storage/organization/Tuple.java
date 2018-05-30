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

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Tuple
        implements Comparable<Tuple>
{
    private final List<Type> types;
    private final List<Object> values;

    public Tuple(Type type, Object value)
    {
        this(ImmutableList.of(type), ImmutableList.of(value));
    }

    public Tuple(List<Type> types, Object... values)
    {
        this(types, ImmutableList.copyOf(values));
    }

    public Tuple(List<Type> types, List<Object> values)
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

    public List<Object> getValues()
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
            Object o1 = values.get(i);
            Object o2 = o.getValues().get(i);

            int result = compare(o1, o2);
            if (result != 0) {
                return result;
            }
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
