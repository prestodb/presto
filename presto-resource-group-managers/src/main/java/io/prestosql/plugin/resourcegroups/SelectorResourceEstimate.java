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
package io.prestosql.plugin.resourcegroups;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.prestosql.spi.session.ResourceEstimates;

import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public final class SelectorResourceEstimate
{
    // Resource estimate range is left-inclusive and right-exclusive.
    private final Optional<Range<Duration>> executionTime;
    private final Optional<Range<Duration>> cpuTime;
    private final Optional<Range<DataSize>> peakMemory;

    @JsonCreator
    public SelectorResourceEstimate(
            @JsonProperty("executionTime") Optional<Range<Duration>> executionTime,
            @JsonProperty("cpuTime") Optional<Range<Duration>> cpuTime,
            @JsonProperty("peakMemory") Optional<Range<DataSize>> peakMemory)
    {
        this.executionTime = requireNonNull(executionTime, "executionTime is null");
        this.cpuTime = requireNonNull(cpuTime, "cpuTime is null");
        this.peakMemory = requireNonNull(peakMemory, "peakMemory is null");
    }

    @JsonProperty
    public Optional<Range<Duration>> getExecutionTime()
    {
        return executionTime;
    }

    @JsonProperty
    public Optional<Range<Duration>> getCpuTime()
    {
        return cpuTime;
    }

    @JsonProperty
    public Optional<Range<DataSize>> getPeakMemory()
    {
        return peakMemory;
    }

    boolean match(ResourceEstimates resourceEstimates)
    {
        if (executionTime.isPresent()) {
            Optional<Duration> executionTimeEstimate = resourceEstimates.getExecutionTime();
            if (!executionTimeEstimate.isPresent() || !executionTime.get().contains(executionTimeEstimate.get())) {
                return false;
            }
        }

        if (cpuTime.isPresent()) {
            Optional<Duration> cpuTimeEstimate = resourceEstimates.getCpuTime();
            if (!cpuTimeEstimate.isPresent() || !cpuTime.get().contains(cpuTimeEstimate.get())) {
                return false;
            }
        }

        if (peakMemory.isPresent()) {
            Optional<DataSize> peakMemoryEstimate = resourceEstimates.getPeakMemory();
            if (!peakMemoryEstimate.isPresent() || !peakMemory.get().contains(peakMemoryEstimate.get())) {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("executionTime", executionTime)
                .add("cpuTime", cpuTime)
                .add("peakMemory", peakMemory)
                .toString();
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(executionTime, cpuTime, peakMemory);
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

        SelectorResourceEstimate other = (SelectorResourceEstimate) o;

        return Objects.equals(this.executionTime, other.executionTime) &&
                Objects.equals(this.cpuTime, other.cpuTime) &&
                Objects.equals(this.peakMemory, other.peakMemory);
    }

    /**
     * Define range [min, max).
     */
    public static class Range<T extends Comparable<T>>
    {
        private final Optional<T> min;
        private final Optional<T> max;

        @JsonCreator
        public Range(
                @JsonProperty("min") Optional<T> min,
                @JsonProperty("max") Optional<T> max)
        {
            this.min = requireNonNull(min, "min is null");
            this.max = requireNonNull(max, "max is null");
        }

        boolean contains(T value)
        {
            return (!min.isPresent() || min.get().compareTo(value) <= 0) &&
                    (!max.isPresent() || max.get().compareTo(value) >= 0);
        }

        @JsonProperty
        public Optional<T> getMin()
        {
            return min;
        }

        @JsonProperty
        public Optional<T> getMax()
        {
            return max;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("min", min)
                    .add("max", max)
                    .toString();
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(min, max);
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

            Range<?> other = (Range<?>) o;

            return Objects.equals(this.min, other.min) && Objects.equals(this.max, other.max);
        }
    }
}
