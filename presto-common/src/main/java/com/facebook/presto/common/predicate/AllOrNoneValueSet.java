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
package com.facebook.presto.common.predicate;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

/**
 * Set that either includes all values, or excludes all values.
 */
public class AllOrNoneValueSet
        implements ValueSet
{
    private final Type type;
    private final boolean all;

    @JsonCreator
    public AllOrNoneValueSet(@JsonProperty("type") Type type, @JsonProperty("all") boolean all)
    {
        this.type = requireNonNull(type, "type is null");
        this.all = all;
    }

    static AllOrNoneValueSet all(Type type)
    {
        return new AllOrNoneValueSet(type, true);
    }

    static AllOrNoneValueSet none(Type type)
    {
        return new AllOrNoneValueSet(type, false);
    }

    @Override
    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @Override
    public boolean isNone()
    {
        return !all;
    }

    @Override
    @JsonProperty
    public boolean isAll()
    {
        return all;
    }

    @Override
    public boolean isSingleValue()
    {
        return false;
    }

    @Override
    public Object getSingleValue()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean containsValue(Object value)
    {
        if (!Primitives.wrap(type.getJavaType()).isInstance(value)) {
            throw new IllegalArgumentException(String.format("Value class %s does not match required Type class %s", value.getClass().getName(), Primitives.wrap(type.getJavaType()).getClass().getName()));
        }
        return all;
    }

    public AllOrNone getAllOrNone()
    {
        return () -> all;
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return new ValuesProcessor()
        {
            @Override
            public <T> T transform(Function<Ranges, T> rangesFunction, Function<DiscreteValues, T> valuesFunction, Function<AllOrNone, T> allOrNoneFunction)
            {
                return allOrNoneFunction.apply(getAllOrNone());
            }

            @Override
            public void consume(Consumer<Ranges> rangesConsumer, Consumer<DiscreteValues> valuesConsumer, Consumer<AllOrNone> allOrNoneConsumer)
            {
                allOrNoneConsumer.accept(getAllOrNone());
            }
        };
    }

    @Override
    public ValueSet intersect(ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all && otherValueSet.all);
    }

    @Override
    public ValueSet union(ValueSet other)
    {
        AllOrNoneValueSet otherValueSet = checkCompatibility(other);
        return new AllOrNoneValueSet(type, all || otherValueSet.all);
    }

    @Override
    public ValueSet complement()
    {
        return new AllOrNoneValueSet(type, !all);
    }

    @Override
    public String toString(SqlFunctionProperties properties)
    {
        return "[" + (all ? "ALL" : "NONE") + "]";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, all);
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
        final AllOrNoneValueSet other = (AllOrNoneValueSet) obj;
        return Objects.equals(this.type, other.type)
                && this.all == other.all;
    }

    private AllOrNoneValueSet checkCompatibility(ValueSet other)
    {
        if (!getType().equals(other.getType())) {
            throw new IllegalArgumentException(String.format("Mismatched types: %s vs %s", getType(), other.getType()));
        }
        if (!(other instanceof AllOrNoneValueSet)) {
            throw new IllegalArgumentException(String.format("ValueSet is not a AllOrNoneValueSet: %s", other.getClass()));
        }
        return (AllOrNoneValueSet) other;
    }
}
