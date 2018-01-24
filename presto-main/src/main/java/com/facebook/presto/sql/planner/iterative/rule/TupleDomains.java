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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;

public final class TupleDomains
{
    private TupleDomains() {}

    public static <T> TupleDomain<T> filter(TupleDomain<T> tupleDomain, Collection<T> columns)
    {
        if (tupleDomain.isNone()) {
            return tupleDomain;
        }
        Set<T> columnsSet = ImmutableSet.copyOf(columns);
        Map<T, Domain> domains = tupleDomain.getDomains().get().entrySet().stream()
                .filter(entry -> columnsSet.contains(entry.getKey()))
                .collect(toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
        return TupleDomain.withColumnDomains(domains);
    }
}
