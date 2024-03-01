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
package com.facebook.presto.hive;

import io.airlift.units.DataSize;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.hash.Hashing.md5;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class CacheQuota
{
    public static final CacheQuota NO_CACHE_CONSTRAINTS = new CacheQuota("NO_IDENTITY", Optional.empty());

    private final String identity;
    private final long identifier;
    private final Optional<DataSize> quota;

    public CacheQuota(String identity, Optional<DataSize> quota)
    {
        this.identity = requireNonNull(identity, "identity is null");
        this.identifier = md5().hashString(identity, UTF_8).asLong();
        this.quota = requireNonNull(quota, "quota is null");
    }

    public String getIdentity()
    {
        return identity;
    }

    public long getIdentifier()
    {
        return identifier;
    }

    public Optional<DataSize> getQuota()
    {
        return quota;
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
        CacheQuota that = (CacheQuota) o;
        return identity.equals(that.identity) && identifier == that.identifier && Objects.equals(quota, that.quota);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(identity, identifier, quota);
    }
}
