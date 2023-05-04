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
package com.facebook.presto.spi.statistics;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Describes plan statistics which are derived from history based optimizer.
 */
public class PlanAnalyticsSourceInfo
        extends SourceInfo
{
    private final Optional<String> hash;

    public PlanAnalyticsSourceInfo(Optional<String> hash)
    {
        this.hash = requireNonNull(hash, "hash is null");
    }

    public Optional<String> getHash()
    {
        return hash;
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
        PlanAnalyticsSourceInfo that = (PlanAnalyticsSourceInfo) o;
        return Objects.equals(hash, that.hash);
    }

    @Override
    public String toString()
    {
        return (hash.isPresent()) ? hash.get() : "";
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(hash);
    }

    @Override
    public boolean isConfident()
    {
        return true;
    }
}
