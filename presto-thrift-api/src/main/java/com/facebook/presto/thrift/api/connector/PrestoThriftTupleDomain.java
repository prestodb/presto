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
package com.facebook.presto.thrift.api.connector;

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.google.common.collect.ImmutableSet;

import javax.annotation.Nullable;

import java.util.Map;
import java.util.Objects;

import static com.facebook.drift.annotations.ThriftField.Requiredness.OPTIONAL;
import static com.facebook.presto.thrift.api.connector.NameValidationUtils.checkValidName;
import static com.google.common.base.MoreObjects.toStringHelper;

@ThriftStruct
public final class PrestoThriftTupleDomain
{
    private final Map<String, PrestoThriftDomain> domains;

    @ThriftConstructor
    public PrestoThriftTupleDomain(@Nullable Map<String, PrestoThriftDomain> domains)
    {
        if (domains != null) {
            for (String name : domains.keySet()) {
                checkValidName(name);
            }
        }
        this.domains = domains;
    }

    /**
     * Return a map of column names to constraints.
     */
    @Nullable
    @ThriftField(value = 1, requiredness = OPTIONAL)
    public Map<String, PrestoThriftDomain> getDomains()
    {
        return domains;
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
        PrestoThriftTupleDomain other = (PrestoThriftTupleDomain) obj;
        return Objects.equals(this.domains, other.domains);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(domains);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("columnsWithConstraints", domains != null ? domains.keySet() : ImmutableSet.of())
                .toString();
    }
}
