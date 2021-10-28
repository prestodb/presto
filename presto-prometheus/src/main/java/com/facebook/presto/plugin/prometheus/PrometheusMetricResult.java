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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;

import java.util.Arrays;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class PrometheusMetricResult
{
    private final Map<String, String> metricHeader;
    private PrometheusTimeSeriesValueArray timeSeriesValues;

    @JsonCreator
    public PrometheusMetricResult(
            @JsonProperty("metric") Map<String, String> metricHeader,
            @JsonProperty("values") PrometheusTimeSeriesValueArray timeSeriesValues)
    {
        requireNonNull(metricHeader, "metricHeader is null");
        this.metricHeader = metricHeader;
        this.timeSeriesValues = timeSeriesValues;
    }

    @JsonSetter("value")
    private final void setTimeSeriesValues(PrometheusTimeSeriesValue timeSeriesValue)
    {
        this.timeSeriesValues = new PrometheusTimeSeriesValueArray(Arrays.asList(timeSeriesValue));
    }

    public Map<String, String> getMetricHeader()
    {
        return metricHeader;
    }

    public PrometheusTimeSeriesValueArray getTimeSeriesValues()
    {
        return timeSeriesValues;
    }
}
