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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

public class ConfidenceBasedTtlInfo
{
    private final Instant expiryInstant;
    private final Double confidencePercentage;

    @JsonCreator
    public ConfidenceBasedTtlInfo(
            @JsonProperty("expiryEpochTime") long expiryEpochTime,
            @JsonProperty("confidencePercentage") double confidencePercentage)
    {
        this.expiryInstant = Instant.ofEpochSecond(expiryEpochTime);
        this.confidencePercentage = confidencePercentage;
    }

    public static ConfidenceBasedTtlInfo getInfiniteTtl()
    {
        return new ConfidenceBasedTtlInfo(Instant.MAX.getEpochSecond(), 100);
    }

    @JsonProperty("expiryEpochTime")
    public long getExpiryEpochSecond()
    {
        return expiryInstant.getEpochSecond();
    }

    public Instant getExpiryInstant()
    {
        return expiryInstant;
    }

    @JsonProperty
    public Double getConfidencePercentage()
    {
        return confidencePercentage;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(expiryInstant, confidencePercentage);
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
        ConfidenceBasedTtlInfo other = (ConfidenceBasedTtlInfo) obj;
        return expiryInstant.equals(other.getExpiryInstant()) &&
                confidencePercentage.equals(other.getConfidencePercentage());
    }

    public String toString()
    {
        return "expiryEpochTimeUTC=" + expiryInstant + ", confidencePercentage=" + confidencePercentage;
    }
}
