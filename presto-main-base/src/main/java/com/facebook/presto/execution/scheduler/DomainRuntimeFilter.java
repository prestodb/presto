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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.Range;
import com.facebook.presto.common.predicate.SortedRangeSet;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A {@link RuntimeFilter} backed by a {@link TupleDomain}.
 *
 * <p>Each instance targets exactly one join column: the underlying domain is either
 * {@code all()}, {@code none()}, or contains exactly one column entry keyed by the
 * filter ID string. {@link #toTupleDomain(String, Type)} re-keys that single entry
 * to the probe column name for use by the connector.
 *
 * <p>Wire form: serialized as a bare {@code TupleDomain} (no {@code @kind} field)
 * for back-compat with older coordinators; deserialized by {@link RuntimeFilterDeserializer}.
 */
public class DomainRuntimeFilter
        implements RuntimeFilter
{
    private final TupleDomain<String> domain;

    public DomainRuntimeFilter(TupleDomain<String> domain)
    {
        this.domain = requireNonNull(domain, "domain is null");
    }

    @JsonValue
    public TupleDomain<String> getDomain()
    {
        return domain;
    }

    @Override
    public RuntimeFilter mergeWith(RuntimeFilter other)
    {
        if (!(other instanceof DomainRuntimeFilter)) {
            throw new IllegalArgumentException(
                    "Cannot merge a DomainRuntimeFilter with " + other.getClass().getSimpleName());
        }
        return new DomainRuntimeFilter(
                TupleDomain.columnWiseUnion(domain, ((DomainRuntimeFilter) other).domain));
    }

    @Override
    public TupleDomain<String> toTupleDomain(String column, Type type)
    {
        if (column.isEmpty() || domain.isAll() || domain.isNone()) {
            return domain;
        }
        Map<String, Domain> domains = domain.getDomains().get();
        checkArgument(domains.size() == 1,
                "Expected single-column domain but got %s columns: %s", domains.size(), domains.keySet());
        Domain single = domains.values().iterator().next();
        return TupleDomain.withColumnDomains(ImmutableMap.of(column, single));
    }

    @Override
    public boolean isAll()
    {
        return domain.isAll();
    }

    @Override
    public boolean isNone()
    {
        return domain.isNone();
    }

    @Override
    public long estimatedRetainedSizeInBytes()
    {
        if (domain.isNone() || domain.isAll()) {
            return 0;
        }
        long totalSize = 0;
        for (Domain columnDomain : domain.getDomains().get().values()) {
            if (!(columnDomain.getValues() instanceof SortedRangeSet)) {
                continue;
            }
            for (Range range : columnDomain.getValues().getRanges().getOrderedRanges()) {
                totalSize += range.getLow().getValueBlock().map(Block::getRetainedSizeInBytes).orElse(0L);
                totalSize += range.getHigh().getValueBlock().map(Block::getRetainedSizeInBytes).orElse(0L);
            }
        }
        return totalSize;
    }
}
