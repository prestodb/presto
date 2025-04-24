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

import static com.facebook.presto.common.Utils.checkNonNegativeLongArgument;
import static io.airlift.units.DataSize.succinctBytes;
import static io.airlift.units.Duration.succinctNanos;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

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

    private final long executionTimeInNanos;
    private final long cpuTimeInNanos;
    private final long peakMemoryInBytes;
    private final long peakTaskMemoryInBytes;

    @ThriftConstructor
    public ResourceEstimates(long executionTimeInNanos, long cpuTimeInNanos, long peakMemoryInBytes, long peakTaskMemoryInBytes)
    {
        this.executionTimeInNanos = checkNonNegativeLongArgument(executionTimeInNanos, "executionTimeInNanos is negative");
        this.cpuTimeInNanos = checkNonNegativeLongArgument(cpuTimeInNanos, "cpuTimeInNanos is negative");
        this.peakMemoryInBytes = checkNonNegativeLongArgument(peakMemoryInBytes, "peakMemoryInBytes is negative");
        this.peakTaskMemoryInBytes = checkNonNegativeLongArgument(peakTaskMemoryInBytes, "peakTaskMemoryInBytes is negative");
    }

    @JsonCreator
    public ResourceEstimates(
            @JsonProperty("executionTime") Optional<Duration> executionTime,
            @JsonProperty("cpuTime") Optional<Duration> cpuTime,
            @JsonProperty("peakMemory") Optional<DataSize> peakMemory,
            @JsonProperty("peakTaskMemory") Optional<DataSize> peakTaskMemory)
    {
        this.executionTimeInNanos = requireNonNull(executionTime, "executionTime is null").map(t -> t.roundTo(NANOSECONDS)).orElse(0L);
        this.cpuTimeInNanos = requireNonNull(cpuTime, "cpuTime is null").map(t -> t.roundTo(NANOSECONDS)).orElse(0L);
        this.peakMemoryInBytes = requireNonNull(peakMemory, "peakMemory is null").map(DataSize::toBytes).orElse(0L);
        this.peakTaskMemoryInBytes = requireNonNull(peakTaskMemory, "peakTaskMemory is null").map(DataSize::toBytes).orElse(0L);
    }

    @JsonProperty
    public Optional<Duration> getExecutionTime()
    {
        return Optional.of(succinctNanos(executionTimeInNanos));
    }

    @ThriftField(1)
    public long getExecutionTimeInNanos()
    {
        return executionTimeInNanos;
    }

    @JsonProperty
    public Optional<Duration> getCpuTime()
    {
        return Optional.of(succinctNanos(cpuTimeInNanos));
    }

    @ThriftField(2)
    public long getCpuTimeInNanos()
    {
        return cpuTimeInNanos;
    }

    @JsonProperty
    public Optional<DataSize> getPeakMemory()
    {
        return Optional.of(succinctBytes(peakMemoryInBytes));
    }

    @ThriftField(3)
    public long getPeakMemoryInBytes()
    {
        return peakMemoryInBytes;
    }

    @JsonProperty
    public Optional<DataSize> getPeakTaskMemory()
    {
        return Optional.of(succinctBytes(peakTaskMemoryInBytes));
    }

    @ThriftField(4)
    public long getPeakTaskMemoryInBytes()
    {
        return peakTaskMemoryInBytes;
    }

    @Override
    public String toString()
    {
        final StringBuilder sb = new StringBuilder("ResourceEstimates{");
        sb.append("executionTime=").append(succinctNanos(executionTimeInNanos));
        sb.append(", cpuTime=").append(succinctNanos(cpuTimeInNanos));
        sb.append(", peakMemory=").append(succinctBytes(peakMemoryInBytes));
        sb.append(", peakTaskMemory=").append(succinctBytes(peakTaskMemoryInBytes));
        sb.append('}');
        return sb.toString();
    }
}
