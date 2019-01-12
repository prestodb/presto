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
package io.prestosql.plugin.thrift.api;

import io.airlift.drift.annotations.ThriftConstructor;
import io.airlift.drift.annotations.ThriftField;
import io.airlift.drift.annotations.ThriftStruct;
import io.prestosql.plugin.thrift.api.valuesets.PrestoThriftValueSet;
import io.prestosql.spi.predicate.Domain;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.plugin.thrift.api.valuesets.PrestoThriftValueSet.fromValueSet;
import static java.util.Objects.requireNonNull;

@ThriftStruct
public final class PrestoThriftDomain
{
    private final PrestoThriftValueSet valueSet;
    private final boolean nullAllowed;

    @ThriftConstructor
    public PrestoThriftDomain(PrestoThriftValueSet valueSet, boolean nullAllowed)
    {
        this.valueSet = requireNonNull(valueSet, "valueSet is null");
        this.nullAllowed = nullAllowed;
    }

    @ThriftField(1)
    public PrestoThriftValueSet getValueSet()
    {
        return valueSet;
    }

    @ThriftField(2)
    public boolean isNullAllowed()
    {
        return nullAllowed;
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
        PrestoThriftDomain other = (PrestoThriftDomain) obj;
        return Objects.equals(this.valueSet, other.valueSet) &&
                this.nullAllowed == other.nullAllowed;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(valueSet, nullAllowed);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("valueSet", valueSet)
                .add("nullAllowed", nullAllowed)
                .toString();
    }

    public static PrestoThriftDomain fromDomain(Domain domain)
    {
        return new PrestoThriftDomain(fromValueSet(domain.getValues()), domain.isNullAllowed());
    }
}
