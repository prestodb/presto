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

public enum CacheResult
{
    /**
     * The data we're reading is in cache
     */
    HIT,
    /**
     * The data we're reading is not in cache and we have quota to write them to cache
     */
    MISS,
    /**
     * The data we're reading is not in cache and we don't have quota to write them to cache
     */
    CACHE_QUOTA_EXCEED
}
