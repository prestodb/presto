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
package com.facebook.presto.cache;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.hive.CacheQuotaScope;
import io.airlift.units.DataSize;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.Optional;

import static com.facebook.presto.hive.CacheQuotaScope.GLOBAL;

public class CacheConfig
{
    private boolean cachingEnabled;
    private CacheType cacheType;
    private URI baseDirectory;
    private boolean validationEnabled;
    private CacheQuotaScope cacheQuotaScope = GLOBAL;
    private Optional<DataSize> defaultCacheQuota = Optional.empty();

    @Nullable
    public URI getBaseDirectory()
    {
        return baseDirectory;
    }

    @Config("cache.base-directory")
    @ConfigDescription("Base URI to cache data")
    public CacheConfig setBaseDirectory(URI dataURI)
    {
        this.baseDirectory = dataURI;
        return this;
    }

    public boolean isValidationEnabled()
    {
        return validationEnabled;
    }

    @Config("cache.validation-enabled")
    @ConfigDescription("Enable cache validation by comparing with actual data with cached data")
    public CacheConfig setValidationEnabled(boolean validationEnabled)
    {
        this.validationEnabled = validationEnabled;
        return this;
    }

    @Config("cache.enabled")
    @ConfigDescription("Is cache enabled")
    public CacheConfig setCachingEnabled(boolean cachingEnabled)
    {
        this.cachingEnabled = cachingEnabled;
        return this;
    }

    public boolean isCachingEnabled()
    {
        return cachingEnabled;
    }

    @Config("cache.type")
    @ConfigDescription("Caching type")
    public CacheConfig setCacheType(CacheType cacheType)
    {
        this.cacheType = cacheType;
        return this;
    }

    public CacheType getCacheType()
    {
        return cacheType;
    }

    public CacheQuotaScope getCacheQuotaScope()
    {
        return cacheQuotaScope;
    }

    @Config("cache.cache-quota-scope")
    public CacheConfig setCacheQuotaScope(CacheQuotaScope cacheQuotaScope)
    {
        this.cacheQuotaScope = cacheQuotaScope;
        return this;
    }

    public Optional<DataSize> getDefaultCacheQuota()
    {
        return defaultCacheQuota;
    }

    @Config("cache.default-cache-quota")
    public CacheConfig setDefaultCacheQuota(DataSize defaultCacheQuota)
    {
        if (defaultCacheQuota != null) {
            this.defaultCacheQuota = Optional.of(defaultCacheQuota);
        }
        return this;
    }
}
