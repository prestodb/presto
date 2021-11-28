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
package com.facebook.presto.spi.resourceGroups;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class ResourceGroupQueryLimits
{
    private final Optional<Duration> executionTimeLimit;
    private final Optional<DataSize> totalMemoryLimit;
    private final Optional<Duration> cpuTimeLimit;

    public static final ResourceGroupQueryLimits NO_LIMITS = new ResourceGroupQueryLimits(Optional.empty(), Optional.empty(), Optional.empty());

    @JsonCreator
    public ResourceGroupQueryLimits(
            @JsonProperty("executionTimeLimit") Optional<Duration> executionTimeLimit,
            @JsonProperty("totalMemoryLimit") Optional<DataSize> totalMemoryLimit,
            @JsonProperty("cpuTimeLimit") Optional<Duration> cpuTimeLimit)
    {
        this.executionTimeLimit = requireNonNull(executionTimeLimit, "executionTimeLimit is null");
        this.totalMemoryLimit = requireNonNull(totalMemoryLimit, "totalMemoryLimit is null");
        this.cpuTimeLimit = requireNonNull(cpuTimeLimit, "perQueryCpuTime limit is null");
    }

    public Optional<Duration> getExecutionTimeLimit()
    {
        return executionTimeLimit;
    }

    public Optional<DataSize> getTotalMemoryLimit()
    {
        return totalMemoryLimit;
    }

    public Optional<Duration> getCpuTimeLimit()
    {
        return cpuTimeLimit;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof ResourceGroupQueryLimits)) {
            return false;
        }
        ResourceGroupQueryLimits that = (ResourceGroupQueryLimits) other;
        return (executionTimeLimit.equals(that.executionTimeLimit) &&
                totalMemoryLimit.equals(that.totalMemoryLimit) &&
                cpuTimeLimit.equals(that.cpuTimeLimit));
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(executionTimeLimit, totalMemoryLimit, cpuTimeLimit);
    }

    @Override
    public String toString()
    {
        return format("[executionTimeLimit: %s, totalMemoryLimit: %s], cpuTimeLimit: %s]", executionTimeLimit, totalMemoryLimit, cpuTimeLimit);
    }
}
