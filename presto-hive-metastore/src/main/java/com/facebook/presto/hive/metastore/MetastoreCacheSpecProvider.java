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

import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType;
import jakarta.inject.Inject;

import static com.facebook.presto.hive.metastore.AbstractCachingHiveMetastore.MetastoreCacheType.ALL;
import static java.util.Objects.requireNonNull;

public class MetastoreCacheSpecProvider
{
    private final MetastoreClientConfig clientConfig;

    @Inject
    public MetastoreCacheSpecProvider(MetastoreClientConfig clientConfig)
    {
        this.clientConfig = requireNonNull(clientConfig, "clientConfig is null");
    }

    public MetastoreCacheSpec getMetastoreCacheSpec(MetastoreCacheType type)
    {
        boolean enabled = isEnabled(type);
        if (!enabled) {
            return new MetastoreCacheSpec(false, 0, 0, 0);
        }

        long cacheTtlMillis = clientConfig.getMetastoreCacheTtlByType().getOrDefault(
                type, clientConfig.getDefaultMetastoreCacheTtl()).toMillis();
        long refreshIntervalMillis = clientConfig.getMetastoreCacheRefreshIntervalByType().getOrDefault(
                type, clientConfig.getDefaultMetastoreCacheRefreshInterval()).toMillis();

        return new MetastoreCacheSpec(
                true,
                cacheTtlMillis,
                refreshIntervalMillis,
                clientConfig.getMetastoreCacheMaximumSize());
    }

    private boolean isEnabled(MetastoreCacheType type)
    {
        if (!clientConfig.getEnabledCaches().isEmpty()) {
            return clientConfig.getEnabledCaches().contains(type) || clientConfig.getEnabledCaches().contains(ALL);
        }
        if (!clientConfig.getDisabledCaches().isEmpty()) {
            return !(clientConfig.getDisabledCaches().contains(type) || clientConfig.getDisabledCaches().contains(ALL));
        }

        return isEnabledByLegacyMetastoreScope(type);
    }

    private boolean isEnabledByLegacyMetastoreScope(MetastoreCacheType type)
    {
        switch (clientConfig.getMetastoreCacheScope()) {
            case ALL:
                return true;
            case PARTITION:
                return type == MetastoreCacheType.PARTITION || type == MetastoreCacheType.PARTITION_STATISTICS;
            default:
                return false;
        }
    }
}
