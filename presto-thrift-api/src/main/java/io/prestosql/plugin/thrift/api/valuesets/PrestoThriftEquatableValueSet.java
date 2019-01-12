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
package io.prestosql.plugin.thrift.api.valuesets;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.prestosql.plugin.thrift.api.PrestoThriftBlock;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.EquatableValueSet.ValueEntry;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.plugin.thrift.api.PrestoThriftBlock.fromBlock;
import static java.util.Objects.requireNonNull;

/**
 * A set containing values that are uniquely identifiable.
 * Assumes an infinite number of possible values. The values may be collectively included (aka whitelist)
 * or collectively excluded (aka !whitelist).
 * This structure is used with comparable, but not orderable types like "json", "map".
 */
@ThriftStruct
public final class PrestoThriftEquatableValueSet
{
    private final boolean whiteList;
    private final List<PrestoThriftBlock> values;

    @ThriftConstructor
    public PrestoThriftEquatableValueSet(boolean whiteList, List<PrestoThriftBlock> values)
    {
        this.whiteList = whiteList;
        this.values = requireNonNull(values, "values are null");
    }

    @ThriftField(1)
    public boolean isWhiteList()
    {
        return whiteList;
    }

    @ThriftField(2)
    public List<PrestoThriftBlock> getValues()
    {
        return values;
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
        PrestoThriftEquatableValueSet other = (PrestoThriftEquatableValueSet) obj;
        return this.whiteList == other.whiteList &&
                Objects.equals(this.values, other.values);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(whiteList, values);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("whiteList", whiteList)
                .add("values", values)
                .toString();
    }

    public static PrestoThriftEquatableValueSet fromEquatableValueSet(EquatableValueSet valueSet)
    {
        Type type = valueSet.getType();
        Set<ValueEntry> values = valueSet.getEntries();
        List<PrestoThriftBlock> thriftValues = new ArrayList<>(values.size());
        for (ValueEntry value : values) {
            checkState(type.equals(value.getType()), "ValueEntrySet has elements of different types: %s vs %s", type, value.getType());
            thriftValues.add(fromBlock(value.getBlock(), type));
        }
        return new PrestoThriftEquatableValueSet(valueSet.isWhiteList(), thriftValues);
    }
}
