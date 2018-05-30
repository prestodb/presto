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
package com.facebook.presto.spi.predicate;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableCollection;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * A set containing values that are uniquely identifiable.
 * Assumes an infinite number of possible values. The values may be collectively included (aka whitelist)
 * or collectively excluded (aka !whitelist).
 */
public class EquatableValueSet
        implements ValueSet
{
    private final Type type;
    private final boolean whiteList;
    private final Set<ValueEntry> entries;

    @JsonCreator
    public EquatableValueSet(
            @JsonProperty("type") Type type,
            @JsonProperty("whiteList") boolean whiteList,
            @JsonProperty("entries") Set<ValueEntry> entries)
    {
        requireNonNull(type, "type is null");
        requireNonNull(entries, "entries is null");

        if (!type.isComparable()) {
            throw new IllegalArgumentException("Type is not comparable: " + type);
        }
        if (type.isOrderable()) {
            throw new IllegalArgumentException("Use SortedRangeSet instead");
        }
        this.type = type;
        this.whiteList = whiteList;
        this.entries = Collections.unmodifiableSet(new HashSet<>(entries));
    }

    static EquatableValueSet none(Type type)
    {
        return new EquatableValueSet(type, true, Collections.emptySet());
    }

    static EquatableValueSet all(Type type)
    {
        return new EquatableValueSet(type, false, Collections.emptySet());
    }

    static EquatableValueSet of(Type type, Object first, Object... rest)
    {
        HashSet<ValueEntry> set = new HashSet<>(rest.length + 1);
        set.add(ValueEntry.create(type, first));
        for (Object value : rest) {
            set.add(ValueEntry.create(type, value));
        }
        return new EquatableValueSet(type, true, set);
    }

    static EquatableValueSet copyOf(Type type, Collection<Object> values)
    {
        return new EquatableValueSet(type, true, values.stream()
                .map(value -> ValueEntry.create(type, value))
                .collect(toSet()));
    }

    @JsonProperty
    @Override
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public boolean isWhiteList()
    {
        return whiteList;
    }

    @JsonProperty
    public Set<ValueEntry> getEntries()
    {
        return entries;
    }

    public Collection<Object> getValues()
    {
        return unmodifiableCollection(entries.stream()
                .map(ValueEntry::getValue)
                .collect(toList()));
    }

    @Override
    public boolean isNone()
    {
        return whiteList && entries.isEmpty();
    }

    @Override
    public boolean isAll()
    {
        return !whiteList && entries.isEmpty();
    }

    @Override
    public boolean isSingleValue()
    {
        return whiteList && entries.size() == 1;
    }

    @Override
    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("EquatableValueSet does not have just a single value");
        }
        return entries.iterator().next().getValue();
    }

    @Override
    public boolean containsValue(Object value)
    {
        return whiteList == entries.contains(ValueEntry.create(type, value));
    }

    @Override
    public DiscreteValues getDiscreteValues()
    {
        return new DiscreteValues()
        {
            @Override
            public boolean isWhiteList()
            {
                return EquatableValueSet.this.isWhiteList();
            }

            @Override
            public Collection<Object> getValues()
            {
                return EquatableValueSet.this.getValues();
            }
        };
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> valuesFunction, Function<AllOrNone, T> allOrNoneFunction)
            {
                return valuesFunction.apply(getDiscreteValues());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> valuesConsumer, Consumer<AllOrNone> allOrNoneConsumer)
            {
                valuesConsumer.accept(getDiscreteValues());
            }
        };
    }

    @Override
    public EquatableValueSet intersect(ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);

        if (whiteList && otherValueSet.isWhiteList()) {
            return new EquatableValueSet(type, true, intersect(entries, otherValueSet.entries));
        }
        else if (whiteList) {
            return new EquatableValueSet(type, true, subtract(entries, otherValueSet.entries));
        }
        else if (otherValueSet.isWhiteList()) {
            return new EquatableValueSet(type, true, subtract(otherValueSet.entries, entries));
        }
        else {
            return new EquatableValueSet(type, false, union(otherValueSet.entries, entries));
        }
    }

    @Override
    public EquatableValueSet union(ValueSet other)
    {
        EquatableValueSet otherValueSet = checkCompatibility(other);

        if (whiteList && otherValueSet.isWhiteList()) {
            return new EquatableValueSet(type, true, union(entries, otherValueSet.entries));
        }
        else if (whiteList) {
            return new EquatableValueSet(type, false, subtract(otherValueSet.entries, entries));
        }
        else if (otherValueSet.isWhiteList()) {
            return new EquatableValueSet(type, false, subtract(entries, otherValueSet.entries));
        }
        else {
            return new EquatableValueSet(type, false, intersect(otherValueSet.entries, entries));
        }
    }

    @Override
    public EquatableValueSet complement()
    {
        return new EquatableValueSet(type, !whiteList, entries);
    }

    @Override
    public String toString(ConnectorSession session)
    {
        return (whiteList ? "[ " : "EXCLUDES[ ") + entries.stream()
                .map(entry -> type.getObjectValue(session, entry.getBlock(), 0).toString())
                .collect(Collectors.joining(", ")) + " ]";
    }

    private static <T> Set<T> intersect(Set<T> set1, Set<T> set2)
    {
        return set1.stream()
                .filter(set2::contains)
                .collect(toSet());
    }

    private static <T> Set<T> union(Set<T> set1, Set<T> set2)
    {
        return Stream.concat(set1.stream(), set2.stream())
                .collect(toSet());
    }

    private static <T> Set<T> subtract(Set<T> set1, Set<T> set2)
    {
        return set1.stream()
                .filter(value -> !set2.contains(value))
                .collect(toSet());
    }

    private EquatableValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalStateException(String.format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof EquatableValueSet)) {
            throw new IllegalStateException(String.format("ValueSet is not a EquatableValueSet: %s", other.getClass()));
        }
        return (EquatableValueSet) other;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, whiteList, entries);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        final EquatableValueSet other = (EquatableValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.whiteList == other.whiteList
                && Objects.equals(this.entries, other.entries);
    }

    public static class ValueEntry
    {
        private final Type type;
        private final Block block;

        @JsonCreator
        public ValueEntry(
                @JsonProperty("type") Type type,
                @JsonProperty("block") Block block)
        {
            this.type = requireNonNull(type, "type is null");
            this.block = requireNonNull(block, "block is null");

            if (block.getPositionCount() != 1) {
                throw new IllegalArgumentException("Block should only have one position");
            }
        }

        public static ValueEntry create(Type type, Object value)
        {
            return new ValueEntry(type, Utils.nativeValueToBlock(type, value));
        }

        @JsonProperty
        public Type getType()
        {
            return type;
        }

        @JsonProperty
        public Block getBlock()
        {
            return block;
        }

        public Object getValue()
        {
            return Utils.blockToNativeValue(type, block);
        }

        @Override
        public int hashCode()
        {
            return (int) type.hash(block, 0);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            final ValueEntry other = (ValueEntry) obj;
            return Objects.equals(this.type, other.type)
                    && type.equalTo(this.block, 0, other.block, 0);
        }
    }
}
