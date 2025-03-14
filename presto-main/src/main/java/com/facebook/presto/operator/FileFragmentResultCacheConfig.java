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
package com.facebook.presto.operator;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.CompressionCodec;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.airlift.units.MinDataSize;
import io.airlift.units.MinDuration;

import javax.validation.constraints.Min;

import java.net.URI;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.DAYS;

public class FileFragmentResultCacheConfig
{
    private boolean cachingEnabled;
    private URI baseDirectory;
    private CompressionCodec blockEncodingCompressionCodec = CompressionCodec.NONE;

    private int maxCachedEntries = 10_000;
    private Duration cacheTtl = new Duration(2, DAYS);
    private DataSize maxInFlightSize = new DataSize(1, GIGABYTE);
    private DataSize maxSinglePagesSize = new DataSize(500, MEGABYTE);
    private DataSize maxCacheSize = new DataSize(100, GIGABYTE);

    private boolean inputDataStatsEnabled;

    public boolean isCachingEnabled()
    {
        return cachingEnabled;
    }

    @Config("fragment-result-cache.enabled")
    @ConfigDescription("Enable fragment result caching")
    public FileFragmentResultCacheConfig setCachingEnabled(boolean cachingEnabled)
    {
        this.cachingEnabled = cachingEnabled;
        return this;
    }

    public URI getBaseDirectory()
    {
        return baseDirectory;
    }

    @Config("fragment-result-cache.base-directory")
    @ConfigDescription("Base URI to cache data")
    public FileFragmentResultCacheConfig setBaseDirectory(URI baseDirectory)
    {
        this.baseDirectory = baseDirectory;
        return this;
    }

    public CompressionCodec getBlockEncodingCompressionCodec()
    {
        return blockEncodingCompressionCodec;
    }

    @Config("fragment-result-cache.block-encoding-compression-codec")
    @ConfigDescription("Compression codec for block encoding")
    public FileFragmentResultCacheConfig setBlockEncodingCompressionCodec(CompressionCodec blockEncodingCompressionCodec)
    {
        this.blockEncodingCompressionCodec = blockEncodingCompressionCodec;
        return this;
    }

    @Min(0)
    public int getMaxCachedEntries()
    {
        return maxCachedEntries;
    }

    @Config("fragment-result-cache.max-cached-entries")
    @ConfigDescription("Number of entries allowed in cache")
    public FileFragmentResultCacheConfig setMaxCachedEntries(int maxCachedEntries)
    {
        this.maxCachedEntries = maxCachedEntries;
        return this;
    }

    @MinDuration("0s")
    public Duration getCacheTtl()
    {
        return cacheTtl;
    }

    @Config("fragment-result-cache.cache-ttl")
    @ConfigDescription("Time-to-live for a cache entry")
    public FileFragmentResultCacheConfig setCacheTtl(Duration cacheTtl)
    {
        this.cacheTtl = cacheTtl;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getMaxInFlightSize()
    {
        return maxInFlightSize;
    }

    @Config("fragment-result-cache.max-in-flight-size")
    @ConfigDescription("Maximum size of pages in memory waiting to be flushed")
    public FileFragmentResultCacheConfig setMaxInFlightSize(DataSize maxInFlightSize)
    {
        this.maxInFlightSize = maxInFlightSize;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getMaxSinglePagesSize()
    {
        return maxSinglePagesSize;
    }

    @Config("fragment-result-cache.max-single-pages-size")
    @ConfigDescription("Maximum size of pages write to flushed")
    public FileFragmentResultCacheConfig setMaxSinglePagesSize(DataSize maxSinglePagesSize)
    {
        this.maxSinglePagesSize = maxSinglePagesSize;
        return this;
    }

    @MinDataSize("0B")
    public DataSize getMaxCacheSize()
    {
        return maxCacheSize;
    }

    @Config("fragment-result-cache.max-cache-size")
    @ConfigDescription("Maximum on-disk size of this fragment result cache")
    public FileFragmentResultCacheConfig setMaxCacheSize(DataSize maxCacheSize)
    {
        this.maxCacheSize = maxCacheSize;
        return this;
    }

    public boolean isInputDataStatsEnabled()
    {
        return inputDataStatsEnabled;
    }

    @Config("fragment-result-cache.input-data-stats-enabled")
    @ConfigDescription("Enable tracking of the input data size for fragment cache")
    public FileFragmentResultCacheConfig setInputDataStatsEnabled(boolean inputDataStatsEnabled)
    {
        this.inputDataStatsEnabled = inputDataStatsEnabled;
        return this;
    }
}
