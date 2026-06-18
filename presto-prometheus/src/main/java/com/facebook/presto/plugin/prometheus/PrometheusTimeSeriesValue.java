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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import java.time.Instant;

@JsonFormat(shape = JsonFormat.Shape.ARRAY)
public class PrometheusTimeSeriesValue
{
    @JsonDeserialize(using = PrometheusTimestampDeserializer.class)
    private final Instant timestamp;

    @JsonProperty
    private final String value;

    @JsonCreator
    public PrometheusTimeSeriesValue(
            @JsonProperty("timestamp") Instant timestamp,
            @JsonProperty("value") String value)
    {
        this.timestamp = timestamp;
        this.value = value;
    }

    public Instant getTimestamp()
    {
        return timestamp;
    }

    public String getValue()
    {
        return value;
    }
}
