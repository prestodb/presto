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
package com.facebook.presto.sql.gen;

import com.google.common.cache.LoadingCache;
import org.weakref.jmx.Managed;

import static java.util.Objects.requireNonNull;

public class CacheStatsMBean
{
    private final LoadingCache<?, ?> loadingCache;

    public CacheStatsMBean(LoadingCache<?, ?> loadingCache)
    {
        this.loadingCache = requireNonNull(loadingCache, "loadingCache is null");
    }

    @Managed
    public long size()
    {
        return loadingCache.size();
    }

    @Managed
    public Double getHitRate()
    {
        return loadingCache.stats().hitRate();
    }

    @Managed
    public Double getMissRate()
    {
        return loadingCache.stats().missRate();
    }

    @Managed
    public long getRequestCount()
    {
        return loadingCache.stats().requestCount();
    }
}
