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
import com.facebook.presto.spi.predicate.EquatableValueSet.ValueEntry;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashSet;
import java.util.Set;

/**
 * This class is identical to EquatableValueSet with the exception that we do not
 * throw an IllegalArgumentException if type isOrderable, as we expect it to be orderable
 * Likely a better way to handle this.
 * <p>
 * We can't extend EquatableValueSet because we can't catch exception thrown by super constructor.
 */
public class AnyValueSet
        implements ValueSet
{
    private EquatableValueSet equatableValueSet;

    @JsonCreator
    public AnyValueSet(
            @JsonProperty("type") Type type,
            @JsonProperty("whiteList") boolean whiteList,
            @JsonProperty("entries") Set<ValueEntry> entries)
    {
        try {
            equatableValueSet = new EquatableValueSet(type, whiteList, entries);
        }
        catch (IllegalArgumentException e) {
            // Re-throw if type is not comparable
            if (!type.isComparable()) {
                throw e;
            }
        }
    }

    static AnyValueSet of(Type type, Object first, Object... rest)
    {
        HashSet<ValueEntry> set = new HashSet<>(rest.length + 1);
        set.add(ValueEntry.create(type, first));
        for (Object value : rest) {
            set.add(ValueEntry.create(type, value));
        }
        return new AnyValueSet(type, true, set);
    }

    @Override
    public Type getType()
    {
        return equatableValueSet.getType();
    }

    @Override
    public boolean isNone()
    {
        return equatableValueSet.isNone();
    }

    @Override
    public boolean isAny()
    {
        return true;
    }

    @Override
    public boolean isAll()
    {
        return equatableValueSet.isAll();
    }

    @Override
    public boolean isSingleValue()
    {
        return equatableValueSet.isSingleValue();
    }

    @Override
    public Object getSingleValue()
    {
        return equatableValueSet.getSingleValue();
    }

    @Override
    public boolean containsValue(Object value)
    {
        return equatableValueSet.containsValue(value);
    }

    @Override
    public ValuesProcessor getValuesProcessor()
    {
        return equatableValueSet.getValuesProcessor();
    }

    @Override
    public ValueSet intersect(ValueSet other)
    {
        return equatableValueSet.intersect(other);
    }

    @Override
    public ValueSet union(ValueSet other)
    {
        return equatableValueSet.union(other);
    }

    @Override
    public ValueSet complement()
    {
        return equatableValueSet.complement();
    }

    @Override
    public String toString(ConnectorSession session)
    {
        return equatableValueSet.toString(session);
    }
}
