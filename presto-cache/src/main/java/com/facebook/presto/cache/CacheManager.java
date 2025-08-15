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

import com.facebook.presto.hive.CacheQuota;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public interface CacheManager
{
    /**
     * Given {@param request}, check if the data is in cache.
     * If it is in cache, save the data in {@param buffer} starting at {@param offset} and return HIT.
     * If it is not in cache:
     *      1. If there is still cache quota for this table, return MISS
     *      2. Otherwise, return CACHE_QUOTA_EXCEED
     * @return CacheResult
     */
    CacheResult get(FileReadRequest request, byte[] buffer, int offset, CacheQuota cacheQuota);

    /**
     * Save data in cache
     */
    void put(FileReadRequest request, Slice data, CacheQuota cacheQuota);
}
