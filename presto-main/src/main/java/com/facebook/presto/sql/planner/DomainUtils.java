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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Domain;
import com.facebook.presto.spi.SortedRangeSet;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public final class DomainUtils
{
    private DomainUtils()
    {
    }

    public static <T> Map<Symbol, T> columnHandleToSymbol(Map<ColumnHandle, T> columnMap, Map<Symbol, ColumnHandle> assignments)
    {
        Map<ColumnHandle, Symbol> inverseAssignments = ImmutableBiMap.copyOf(assignments).inverse();
        Preconditions.checkArgument(inverseAssignments.keySet().containsAll(columnMap.keySet()), "assignments does not contain all required column handles");
        ImmutableMap.Builder<Symbol, T> builder = ImmutableMap.builder();
        for (Map.Entry<ColumnHandle, T> entry : columnMap.entrySet()) {
            builder.put(inverseAssignments.get(entry.getKey()), entry.getValue());
        }
        return builder.build();
    }

    /**
     * Reduces the number of discrete ranges in the Domain if there are too many.
     */
    public static Domain simplifyDomain(Domain domain)
    {
        if (domain.getRanges().getRangeCount() <= 32) {
            return domain;
        }
        return Domain.create(SortedRangeSet.of(domain.getRanges().getSpan()), domain.isNullAllowed());
    }

    public static Function<Domain, Domain> simplifyDomainFunction()
    {
        return new Function<Domain, Domain>()
        {
            @Override
            public Domain apply(Domain domain)
            {
                return simplifyDomain(domain);
            }
        };
    }
}
