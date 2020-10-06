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
package com.facebook.presto.hive.orc;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.CacheStatsMBean;
import com.facebook.presto.orc.TupleDomainFilter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.Nullable;

import static com.facebook.presto.orc.TupleDomainFilterUtils.toFilter;
import static java.lang.System.identityHashCode;

public final class TupleDomainFilterCache
{
    private final LoadingCache<DomainCacheKey, TupleDomainFilter> cache = CacheBuilder.newBuilder()
            .recordStats()
            .maximumSize(10_000)
            .build(CacheLoader.from(cacheKey -> toFilter(cacheKey.getDomain())));
    private final CacheStatsMBean cacheStats = new CacheStatsMBean(cache);

    public TupleDomainFilter getFilter(Domain domain)
    {
        return cache.getUnchecked(new DomainCacheKey(domain));
    }

    @Nullable
    @Managed
    @Nested
    public CacheStatsMBean getCache()
    {
        return cacheStats;
    }

    /**
     * Converting Domain into TupleDomainFilter is expensive, hence, we use a cache keyed on Domain.
     * However, by default, the cache uses Domain.hashCode and Domain.equals to check if key already
     * exists. These methods are also expensive. Hence, we use this wrapper to replace Domain.hashCode
     * and Domain.equals with much cheaper identity hashCode and equals. These work because the Domain
     * objects come from HiveTableLayoutHandle which is reused across splits.
     */
    private static final class DomainCacheKey
    {
        private final Domain domain;

        private DomainCacheKey(Domain domain)
        {
            this.domain = domain;
        }

        public Domain getDomain()
        {
            return domain;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DomainCacheKey cacheKey = (DomainCacheKey) o;
            return domain == cacheKey.domain;
        }

        @Override
        public int hashCode()
        {
            return identityHashCode(domain);
        }
    }
}
