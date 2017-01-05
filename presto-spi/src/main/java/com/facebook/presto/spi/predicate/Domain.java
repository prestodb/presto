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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

/**
 * Defines the possible values of a single variable in terms of its valid scalar values and nullability.
 * <p>
 * For example:
 * <p>
 * <ul>
 * <li>Domain.none() => no scalar values allowed, NULL not allowed
 * <li>Domain.all() => all scalar values allowed, NULL allowed
 * <li>Domain.onlyNull() => no scalar values allowed, NULL allowed
 * <li>Domain.notNull() => all scalar values allowed, NULL not allowed
 * </ul>
 * <p>
 */
public final class Domain
{
    private final ValueSet values;
    private final boolean nullAllowed;

    private Domain(ValueSet values, boolean nullAllowed)
    {
        this.values = requireNonNull(values, "values is null");
        this.nullAllowed = nullAllowed;
    }

    @JsonCreator
    public static Domain create(
            @JsonProperty("values") ValueSet values,
            @JsonProperty("nullAllowed") boolean nullAllowed)
    {
        return new Domain(values, nullAllowed);
    }

    public static Domain none(Type type)
    {
        return new Domain(ValueSet.none(type), false);
    }

    public static Domain all(Type type)
    {
        return new Domain(ValueSet.all(type), true);
    }

    public static Domain onlyNull(Type type)
    {
        return new Domain(ValueSet.none(type), true);
    }

    public static Domain notNull(Type type)
    {
        return new Domain(ValueSet.all(type), false);
    }

    public static Domain singleValue(Type type, Object value)
    {
        return new Domain(ValueSet.of(type, value), false);
    }

    public static Domain multipleValues(Type type, List<Object> values)
    {
        if (values.isEmpty()) {
            throw new IllegalArgumentException("values cannot be empty");
        }
        if (values.size() == 1) {
            return singleValue(type, values.get(0));
        }
        return new Domain(ValueSet.of(type, values.get(0), values.subList(1, values.size()).toArray()), false);
    }

    public Type getType()
    {
        return values.getType();
    }

    @JsonProperty
    public ValueSet getValues()
    {
        return values;
    }

    @JsonProperty
    public boolean isNullAllowed()
    {
        return nullAllowed;
    }

    public boolean isNone()
    {
        return values.isNone() && !nullAllowed;
    }

    public boolean isAll()
    {
        return values.isAll() && nullAllowed;
    }

    public boolean isSingleValue()
    {
        return !nullAllowed && values.isSingleValue();
    }

    public boolean isNullableSingleValue()
    {
        if (nullAllowed) {
            return values.isNone();
        }
        else {
            return values.isSingleValue();
        }
    }

    public boolean isOnlyNull()
    {
        return values.isNone() && nullAllowed;
    }

    public Object getSingleValue()
    {
        if (!isSingleValue()) {
            throw new IllegalStateException("Domain is not a single value");
        }
        return values.getSingleValue();
    }

    public Object getNullableSingleValue()
    {
        if (!isNullableSingleValue()) {
            throw new IllegalStateException("Domain is not a nullable single value");
        }

        if (nullAllowed) {
            return null;
        }
        else {
            return values.getSingleValue();
        }
    }

    public boolean includesNullableValue(Object value)
    {
        return value == null ? nullAllowed : values.containsValue(value);
    }

    public boolean overlaps(Domain other)
    {
        checkCompatibility(other);
        return !this.intersect(other).isNone();
    }

    public boolean contains(Domain other)
    {
        checkCompatibility(other);
        return this.union(other).equals(this);
    }

    public Domain intersect(Domain other)
    {
        checkCompatibility(other);
        return new Domain(values.intersect(other.getValues()), this.isNullAllowed() && other.isNullAllowed());
    }

    public Domain union(Domain other)
    {
        checkCompatibility(other);
        return new Domain(values.union(other.getValues()), this.isNullAllowed() || other.isNullAllowed());
    }

    public static Domain union(List<Domain> domains)
    {
        if (domains.isEmpty()) {
            throw new IllegalArgumentException("domains cannot be empty for union");
        }
        if (domains.size() == 1) {
            return domains.get(0);
        }

        boolean nullAllowed = false;
        List<ValueSet> valueSets = new ArrayList<>(domains.size());
        for (Domain domain : domains) {
            valueSets.add(domain.getValues());
            nullAllowed = nullAllowed || domain.nullAllowed;
        }

        ValueSet unionedValues = valueSets.get(0).union(valueSets.subList(1, valueSets.size()));

        return new Domain(unionedValues, nullAllowed);
    }

    public Domain complement()
    {
        return new Domain(values.complement(), !nullAllowed);
    }

    public Domain subtract(Domain other)
    {
        checkCompatibility(other);
        return new Domain(values.subtract(other.getValues()), this.isNullAllowed() && !other.isNullAllowed());
    }

    private void checkCompatibility(Domain domain)
    {
        if (!getType().equals(domain.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched Domain types: %s vs %s", getType(), domain.getType()));
        }
        if (values.getClass() != domain.values.getClass()) {
            throw new IllegalArgumentException(String.format("Mismatched Domain value set classes: %s vs %s", values.getClass(), domain.values.getClass()));
        }
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(values, nullAllowed);
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
        Domain other = (Domain) obj;
        return Objects.equals(this.values, other.values) &&
                this.nullAllowed == other.nullAllowed;
    }

    public String toString(ConnectorSession session)
    {
        return "[ " + (nullAllowed ? "NULL, " : "") + values.toString(session) + " ]";
    }
}
