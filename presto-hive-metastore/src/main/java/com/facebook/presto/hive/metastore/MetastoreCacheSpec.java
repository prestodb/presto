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
package com.facebook.presto.hive.metastore;

public class MetastoreCacheSpec
{
    private final boolean enabled;
    private final long cacheTtlMillis;
    private final long refreshIntervalMillis;
    private final long maximumSize;

    public MetastoreCacheSpec(boolean enabled, long cacheTtlMillis, long refreshIntervalMillis, long maximumSize)
    {
        this.enabled = enabled;
        this.cacheTtlMillis = cacheTtlMillis;
        this.refreshIntervalMillis = refreshIntervalMillis;
        this.maximumSize = maximumSize;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    public long getCacheTtlMillis()
    {
        return cacheTtlMillis;
    }

    public long getRefreshIntervalMillis()
    {
        return refreshIntervalMillis;
    }

    public long getMaximumSize()
    {
        return maximumSize;
    }
}
