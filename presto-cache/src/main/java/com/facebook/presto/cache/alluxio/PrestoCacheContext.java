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
package com.facebook.presto.cache.alluxio;

import alluxio.client.file.CacheContext;
import alluxio.client.quota.CacheQuota;
import alluxio.client.quota.CacheScope;
import com.facebook.presto.hive.HiveFileContext;
import com.google.common.collect.ImmutableMap;

import static com.facebook.presto.common.RuntimeUnit.BYTE;
import static com.facebook.presto.common.RuntimeUnit.NANO;
import static com.facebook.presto.common.RuntimeUnit.NONE;
import static java.util.Objects.requireNonNull;

public class PrestoCacheContext
        extends CacheContext
{
    private final HiveFileContext hiveFileContext;

    public static PrestoCacheContext build(String cacheIdentifier, HiveFileContext hiveFileContext, boolean cacheQuotaEnabled)
    {
        PrestoCacheContext context = new PrestoCacheContext(hiveFileContext);
        context.setCacheIdentifier(cacheIdentifier);
        if (cacheQuotaEnabled) {
            CacheScope scope = CacheScope.create(hiveFileContext.getCacheQuota().getIdentity());
            context.setCacheScope(scope);
            if (hiveFileContext.getCacheQuota().getQuota().isPresent()) {
                context.setCacheQuota(new CacheQuota(ImmutableMap.of(scope.level(), hiveFileContext.getCacheQuota().getQuota().get().toBytes())));
            }
            else {
                context.setCacheQuota(CacheQuota.UNLIMITED);
            }
        }
        return context;
    }
    private PrestoCacheContext(HiveFileContext hiveFileContext)
    {
        this.hiveFileContext = requireNonNull(hiveFileContext, "hiveFileContext is null");
    }

    @Override
    public void incrementCounter(String name, StatsUnit unit, long value)
    {
        switch (unit) {
            case BYTE:
                hiveFileContext.incrementCounter(name, BYTE, value);
                break;
            case NANO:
                hiveFileContext.incrementCounter(name, NANO, value);
                break;
            default:
                hiveFileContext.incrementCounter(name, NONE, value);
        }
    }

    public HiveFileContext getHiveFileContext()
    {
        return hiveFileContext;
    }
}
