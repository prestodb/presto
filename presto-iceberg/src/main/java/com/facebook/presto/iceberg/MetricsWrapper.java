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
package com.facebook.presto.iceberg;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.iceberg.Metrics;

import java.nio.ByteBuffer;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class MetricsWrapper
{
    private final Metrics metrics;

    @JsonCreator
    public MetricsWrapper(
            @JsonProperty("recordCount") Long recordCount,
            @JsonProperty("columnSizes") Map<Integer, Long> columnSizes,
            @JsonProperty("valueCounts") Map<Integer, Long> valueCounts,
            @JsonProperty("nullValueCounts") Map<Integer, Long> nullValueCounts,
            @JsonProperty("lowerBounds") Map<Integer, ByteBuffer> lowerBounds,
            @JsonProperty("upperBounds") Map<Integer, ByteBuffer> upperBounds)
    {
        this(new Metrics(
                recordCount,
                columnSizes,
                valueCounts,
                nullValueCounts,
                lowerBounds,
                upperBounds));
    }

    public MetricsWrapper(Metrics metrics)
    {
        this.metrics = requireNonNull(metrics, "metrics is null");
    }

    public Metrics metrics()
    {
        return metrics;
    }

    @JsonProperty
    public Long recordCount()
    {
        return metrics.recordCount();
    }

    @JsonProperty
    public Map<Integer, Long> columnSizes()
    {
        return metrics.columnSizes();
    }

    @JsonProperty
    public Map<Integer, Long> valueCounts()
    {
        return metrics.valueCounts();
    }

    @JsonProperty
    public Map<Integer, Long> nullValueCounts()
    {
        return metrics.nullValueCounts();
    }

    @JsonProperty
    public Map<Integer, ByteBuffer> lowerBounds()
    {
        return metrics.lowerBounds();
    }

    @JsonProperty
    public Map<Integer, ByteBuffer> upperBounds()
    {
        return metrics.upperBounds();
    }
}
