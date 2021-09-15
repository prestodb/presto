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
package com.facebook.presto.spi.ttl;

import java.time.Instant;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class NodeTtl
{
    private final Set<ConfidenceBasedTtlInfo> ttls;
    private final Instant ttlPredictionInstant;

    public NodeTtl(Set<ConfidenceBasedTtlInfo> ttls)
    {
        this(ttls, Instant.now());
    }

    public NodeTtl(Set<ConfidenceBasedTtlInfo> ttls, Instant ttlPredictionEpochTime)
    {
        this.ttls = requireNonNull(ttls, "ttls is null");
        this.ttlPredictionInstant = requireNonNull(ttlPredictionEpochTime, "ttlPredictionEpochTime is null");
    }

    public Set<ConfidenceBasedTtlInfo> getTtlInfo()
    {
        return ttls;
    }

    public Instant getTtlPredictionInstant()
    {
        return ttlPredictionInstant;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(ttls);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NodeTtl other = (NodeTtl) obj;
        return this.getTtlInfo().equals(other.getTtlInfo());
    }

    public String toString()
    {
        return ttls.toString();
    }
}
