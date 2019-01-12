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
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Collection;

import static java.util.stream.Collectors.toList;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = EquatableValueSet.class, name = "equatable"),
        @JsonSubTypes.Type(value = SortedRangeSet.class, name = "sortable"),
        @JsonSubTypes.Type(value = AllOrNoneValueSet.class, name = "allOrNone")})
public interface ValueSet
{
    static ValueSet none(Type type)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.none(type);
        }
        if (type.isComparable()) {
            return EquatableValueSet.none(type);
        }
        return AllOrNoneValueSet.none(type);
    }

    static ValueSet all(Type type)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.all(type);
        }
        if (type.isComparable()) {
            return EquatableValueSet.all(type);
        }
        return AllOrNoneValueSet.all(type);
    }

    static ValueSet of(Type type, Object first, Object... rest)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.of(type, first, rest);
        }
        if (type.isComparable()) {
            return EquatableValueSet.of(type, first, rest);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet copyOf(Type type, Collection<Object> values)
    {
        if (type.isOrderable()) {
            return SortedRangeSet.copyOf(type, values.stream()
                    .map(value -> Range.equal(type, value))
                    .collect(toList()));
        }
        if (type.isComparable()) {
            return EquatableValueSet.copyOf(type, values);
        }
        throw new IllegalArgumentException("Cannot create discrete ValueSet with non-comparable type: " + type);
    }

    static ValueSet ofRanges(Range first, Range... rest)
    {
        return SortedRangeSet.of(first, rest);
    }

    static ValueSet copyOfRanges(Type type, Collection<Range> ranges)
    {
        return SortedRangeSet.copyOf(type, ranges);
    }

    Type getType();

    boolean isNone();

    boolean isAll();

    boolean isSingleValue();

    Object getSingleValue();

    boolean containsValue(Object value);

    /**
     * @return value predicates for equatable Types (but not orderable)
     */
    default DiscreteValues getDiscreteValues()
    {
        throw new UnsupportedOperationException();
    }

    /**
     * @return range predicates for orderable Types
     */
    default Ranges getRanges()
    {
        throw new UnsupportedOperationException();
    }

    ValuesProcessor getValuesProcessor();

    ValueSet intersect(ValueSet other);

    ValueSet union(ValueSet other);

    default ValueSet union(Collection<ValueSet> valueSets)
    {
        ValueSet current = this;
        for (ValueSet valueSet : valueSets) {
            current = current.union(valueSet);
        }
        return current;
    }

    ValueSet complement();

    default boolean overlaps(ValueSet other)
    {
        return !this.intersect(other).isNone();
    }

    default ValueSet subtract(ValueSet other)
    {
        return this.intersect(other.complement());
    }

    default boolean contains(ValueSet other)
    {
        return this.union(other).equals(this);
    }

    String toString(ConnectorSession session);
}
