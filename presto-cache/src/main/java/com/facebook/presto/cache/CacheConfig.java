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
import io.airlift.units.DataSize;

import java.net.URI;

import static io.airlift.units.DataSize.Unit.GIGABYTE;

public class CacheConfig
{
    private boolean cachingEnabled;
    private CacheType cacheType;
    private URI baseDirectory;
    private boolean validationEnabled;
    private DataSize maxCacheSize = new DataSize(2, GIGABYTE);

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

    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("cache.max-cache-size")
    @ConfigDescription("The maximum cache size")
    public CacheConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }
}
