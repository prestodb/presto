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
package com.facebook.presto.plugin.prometheus;

import java.time.Instant;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrometheusStandardizedRow
{
    private final Map<String, String> labels;
    private final Instant timestamp;
    private final Double value;

    public PrometheusStandardizedRow(Map<String, String> labels, Instant timestamp, Double value)
    {
        this.labels = requireNonNull(labels, "labels is null");
        this.timestamp = requireNonNull(timestamp, "timestamp is null");
        this.value = requireNonNull(value, "value is null");
    }

    public Map<String, String> getLabels()
    {
        return labels;
    }

    public Instant getTimestamp()
    {
        return timestamp;
    }

    public Double getValue()
    {
        return value;
    }
}
