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
package com.facebook.presto.spi.session;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.units.DataSize.succinctBytes;
import static java.util.Objects.requireNonNull;

/**
 * Estimated resource usage for a query.
 * <p>
 * This class is under active development and should be considered beta.
 */
public final class ResourceEstimates
{
    public static final String EXECUTION_TIME = "EXECUTION_TIME";
    public static final String CPU_TIME = "CPU_TIME";
    public static final String PEAK_MEMORY = "PEAK_MEMORY";

    private final Optional<Duration> executionTime;
    private final Optional<Duration> cpuTime;
    private final Optional<DataSize> peakMemory;

    @JsonCreator
    public ResourceEstimates(
            @JsonProperty("executionTime") Optional<Duration> executionTime,
            @JsonProperty("cpuTime") Optional<Duration> cpuTime,
            @JsonProperty("peakMemory") Optional<DataSize> peakMemory)
    {
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.peakMemory = requireNonNull(peakMemory, "peakMemory is null");
    }

    @JsonProperty
    public Optional<Duration> getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public Optional<Duration> getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public Optional<DataSize> getPeakMemory()
    {
        return peakMemory;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("ResourceEstimates{");
        sb.append("executionTime=").append(executionTime);
        sb.append(", cpuTime=").append(cpuTime);
        sb.append(", peakMemory=").append(peakMemory);
        sb.append('}');
        return sb.toString();
    }

    public ResourceEstimates add(ResourceEstimates other)
    {
        Optional<Duration> newExecutionTime = Optional.empty();
        if (executionTime.isPresent() && other.executionTime.isPresent()) {
            newExecutionTime =
                    Optional.of(new Duration(executionTime.map(Duration::toMillis).orElse(0L) + other.executionTime.map(Duration::toMillis).orElse(0L), TimeUnit.MILLISECONDS));
        }

        Optional<Duration> newCpuTime = Optional.empty();
        if (cpuTime.isPresent() && other.cpuTime.isPresent()) {
            newCpuTime = Optional.of(new Duration(cpuTime.map(Duration::toMillis).orElse(0L) + other.cpuTime.map(Duration::toMillis).orElse(0L), TimeUnit.MILLISECONDS));
        }

        Optional<DataSize> newPeakMemory = Optional.empty();
        if (peakMemory.isPresent() && other.peakMemory.isPresent()) {
            newPeakMemory = Optional.of(succinctBytes(peakMemory.map(DataSize::toBytes).orElse(0L) + other.peakMemory.map(DataSize::toBytes).orElse(0L)));
        }

        return new ResourceEstimates(newExecutionTime, newCpuTime, newPeakMemory);
    }

    public ResourceEstimates withDefaults(ResourceEstimates defaults)
    {
        Optional<Duration> newExecutionTime = Optional.empty();
        if (!executionTime.isPresent() && defaults.executionTime.isPresent()) {
            newExecutionTime = defaults.executionTime;
        }

        Optional<Duration> newCpuTime = Optional.empty();
        if (!cpuTime.isPresent() && defaults.cpuTime.isPresent()) {
            newCpuTime = defaults.cpuTime;
        }

        Optional<DataSize> newPeakMemory = Optional.empty();
        if (!peakMemory.isPresent() && defaults.peakMemory.isPresent()) {
            newPeakMemory = defaults.peakMemory;
        }

        return new ResourceEstimates(newExecutionTime, newCpuTime, newPeakMemory);
    }
}
