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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Estimated resource usage for a query.
 * <p>
 * This class is under active development and should be considered beta.
 */
@ThriftStruct
public final class ResourceEstimates
{
    public static final String EXECUTION_TIME = "EXECUTION_TIME";
    public static final String CPU_TIME = "CPU_TIME";
    public static final String PEAK_MEMORY = "PEAK_MEMORY";
    public static final String PEAK_TASK_MEMORY = "PEAK_TASK_MEMORY";

    private final Optional<Duration> executionTime;
    private final Optional<Duration> cpuTime;
    private final Optional<DataSize> peakMemory;
    private final Optional<DataSize> peakTaskMemory;

    @ThriftConstructor
    @JsonCreator
    public ResourceEstimates(
            @JsonProperty("executionTime") Optional<Duration> executionTime,
            @JsonProperty("cpuTime") Optional<Duration> cpuTime,
            @JsonProperty("peakMemory") Optional<DataSize> peakMemory,
            @JsonProperty("peakTaskMemory") Optional<DataSize> peakTaskMemory)
    {
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.peakMemory = requireNonNull(peakMemory, "peakMemory is null");
        this.peakTaskMemory = requireNonNull(peakTaskMemory, "peakTaskMemory is null");
    }

    @ThriftField(1)
    @JsonProperty
    public Optional<Duration> getExecutionTime()
    {
        return executionTime;
    }

    @ThriftField(2)
    @JsonProperty
    public Optional<Duration> getCpuTime()
    {
        return cpuTime;
    }

    @ThriftField(3)
    @JsonProperty
    public Optional<DataSize> getPeakMemory()
    {
        return peakMemory;
    }

    @ThriftField(4)
    @JsonProperty
    public Optional<DataSize> getPeakTaskMemory()
    {
        return peakTaskMemory;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("ResourceEstimates{");
        sb.append("executionTime=").append(executionTime);
        sb.append(", cpuTime=").append(cpuTime);
        sb.append(", peakMemory=").append(peakMemory);
        sb.append(", peakTaskMemory=").append(peakTaskMemory);
        sb.append('}');
        return sb.toString();
    }
}
