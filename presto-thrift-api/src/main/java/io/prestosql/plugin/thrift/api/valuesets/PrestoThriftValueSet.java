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
import io.prestosql.spi.predicate.AllOrNoneValueSet;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.ValueSet;

import javax.annotation.Nullable;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static io.prestosql.plugin.thrift.api.valuesets.PrestoThriftAllOrNoneValueSet.fromAllOrNoneValueSet;
import static io.prestosql.plugin.thrift.api.valuesets.PrestoThriftEquatableValueSet.fromEquatableValueSet;
import static io.prestosql.plugin.thrift.api.valuesets.PrestoThriftRangeValueSet.fromSortedRangeSet;

@ThriftStruct
public final class PrestoThriftValueSet
{
    private final PrestoThriftAllOrNoneValueSet allOrNoneValueSet;
    private final PrestoThriftEquatableValueSet equatableValueSet;
    private final PrestoThriftRangeValueSet rangeValueSet;

    @ThriftConstructor
    public PrestoThriftValueSet(
            @Nullable PrestoThriftAllOrNoneValueSet allOrNoneValueSet,
            @Nullable PrestoThriftEquatableValueSet equatableValueSet,
            @Nullable PrestoThriftRangeValueSet rangeValueSet)
    {
        checkArgument(isExactlyOneNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet), "exactly one value set must be present");
        this.allOrNoneValueSet = allOrNoneValueSet;
        this.equatableValueSet = equatableValueSet;
        this.rangeValueSet = rangeValueSet;
    }

    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public PrestoThriftAllOrNoneValueSet getAllOrNoneValueSet()
    {
        return allOrNoneValueSet;
    }

    @Nullable
    @ThriftField(value = 2, requiredness = OPTIONAL)
    public PrestoThriftEquatableValueSet getEquatableValueSet()
    {
        return equatableValueSet;
    }

    @Nullable
    @ThriftField(value = 3, requiredness = OPTIONAL)
    public PrestoThriftRangeValueSet getRangeValueSet()
    {
        return rangeValueSet;
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
        PrestoThriftValueSet other = (PrestoThriftValueSet) obj;
        return Objects.equals(this.allOrNoneValueSet, other.allOrNoneValueSet) &&
                Objects.equals(this.equatableValueSet, other.equatableValueSet) &&
                Objects.equals(this.rangeValueSet, other.rangeValueSet);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(allOrNoneValueSet, equatableValueSet, rangeValueSet);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("valueSet", firstNonNull(allOrNoneValueSet, equatableValueSet, rangeValueSet))
                .toString();
    }

    public static PrestoThriftValueSet fromValueSet(ValueSet valueSet)
    {
        if (valueSet.getClass() == AllOrNoneValueSet.class) {
            return new PrestoThriftValueSet(
                    fromAllOrNoneValueSet((AllOrNoneValueSet) valueSet),
                    null,
                    null);
        }
        else if (valueSet.getClass() == EquatableValueSet.class) {
            return new PrestoThriftValueSet(
                    null,
                    fromEquatableValueSet((EquatableValueSet) valueSet),
                    null);
        }
        else if (valueSet.getClass() == SortedRangeSet.class) {
            return new PrestoThriftValueSet(
                    null,
                    null,
                    fromSortedRangeSet((SortedRangeSet) valueSet));
        }
        else {
            throw new IllegalArgumentException("Unknown implementation of a value set: " + valueSet.getClass());
        }
    }

    private static boolean isExactlyOneNonNull(Object a, Object b, Object c)
    {
        return a != null && b == null && c == null ||
                a == null && b != null && c == null ||
                a == null && b == null && c != null;
    }

    private static Object firstNonNull(Object a, Object b, Object c)
    {
        if (a != null) {
            return a;
        }
        if (b != null) {
            return b;
        }
        if (c != null) {
            return c;
        }
        throw new IllegalArgumentException("All arguments are null");
    }
}
