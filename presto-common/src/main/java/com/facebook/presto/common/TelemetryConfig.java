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
package com.facebook.presto.common;

import java.util.Map;

import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;

/**
 * The type TelemetryConfig to store all the values in telemetry.properties.
 */
public class TelemetryConfig
{
    private static TelemetryConfig telemetryConfig;

    private String tracingBackendUrl;
    private Integer maxExporterBatchSize;
    private Integer maxQueueSize;
    private Integer exporterTimeout;
    private Integer scheduleDelay;
    private Double samplingRatio;
    private Boolean tracingEnabled = false;
    private Boolean spanSampling = false;

    /**
     * The type Telemetry config constants.
     */
    public static class TelemetryConfigConstants
    {
        private static final String TRACING_ENABLED = "tracing-enabled";
        private static final String TRACING_BACKEND_URL = "tracing-backend-url";
        private static final String MAX_EXPORTER_BATCH_SIZE = "max-exporter-batch-size";
        private static final String MAX_QUEUE_SIZE = "max-queue-size";
        private static final String SCHEDULE_DELAY = "schedule-delay";
        private static final String EXPORTER_TIMEOUT = "exporter-timeout";
        private static final String TRACE_SAMPLING_RATIO = "trace-sampling-ratio";
        private static final String SPAN_SAMPLING = "span-sampling";
    }

    private TelemetryConfig()
    {
    }

    /**
     * Gets telemetry config.
     *
     * @return the telemetry config
     */
    public static TelemetryConfig getTelemetryConfig()
    {   // prevent multiple instance creation
        telemetryConfig = nonNull(telemetryConfig) ? telemetryConfig : new TelemetryConfig();
        return telemetryConfig;
    }

    /**
     * Sets telemetry properties.
     *
     * @param telemetryProperties the telemetry properties
     */
    public void setTelemetryProperties(Map<String, String> telemetryProperties)
    {
        tracingBackendUrl = requireNonNull(telemetryProperties.get(TelemetryConfigConstants.TRACING_BACKEND_URL), "exporter endpoint cant be null");
        maxQueueSize = Integer.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.MAX_QUEUE_SIZE), "max queue size cant be null"));
        maxExporterBatchSize = Integer.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.MAX_EXPORTER_BATCH_SIZE), "max exporter batch size cant be null"));
        exporterTimeout = Integer.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.EXPORTER_TIMEOUT), "exporter timeout cant be null"));
        tracingEnabled = Boolean.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.TRACING_ENABLED), "trace enabled cant be null"));
        scheduleDelay = Integer.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.SCHEDULE_DELAY), "schedule delay cant be null"));
        samplingRatio = Double.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.TRACE_SAMPLING_RATIO), "sampling ratio cant be null"));
        spanSampling = Boolean.valueOf(requireNonNull(telemetryProperties.get(TelemetryConfigConstants.SPAN_SAMPLING), "span sampling must be provided"));
    }

    /**
     * Sets tracing enabled. For dynamically enable/disable from /v1/telemetry/config endpoint
     *
     * @param tracingEnabled the tracing enabled
     */
    public void setTracingEnabled(Boolean tracingEnabled)
    {
        getTelemetryConfig().tracingEnabled = tracingEnabled;
    }

    /**
     * Sets span sampling.
     *
     * @param spanSampling the span sampling
     */
    public void setSpanSampling(Boolean spanSampling)
    {
        getTelemetryConfig().spanSampling = spanSampling;
    }

    /**
     * Gets exporter endpoint.
     *
     * @return the exporter endpoint
     */
    public String getTracingBackendUrl()
    {
        return this.tracingBackendUrl;
    }

    /**
     * Gets max exporter batch size.
     *
     * @return the max exporter batch size
     */
    public Integer getMaxExporterBatchSize()
    {
        return this.maxExporterBatchSize;
    }

    /**
     * Gets max queue size.
     *
     * @return the max queue size
     */
    public Integer getMaxQueueSize()
    {
        return this.maxQueueSize;
    }

    /**
     * Gets exporter timeout.
     *
     * @return the exporter timeout
     */
    public Integer getExporterTimeout()
    {
        return this.exporterTimeout;
    }

    /**
     * Gets schedule delay.
     *
     * @return the schedule delay
     */
    public Integer getScheduleDelay()
    {
        return this.scheduleDelay;
    }

    /**
     * Gets sampling ratio.
     *
     * @return the sampling ratio
     */
    public Double getSamplingRatio()
    {
        return this.samplingRatio;
    }

    /**
     * Gets tracing enabled.
     *
     * @return the tracing enabled
     */
    public static Boolean getTracingEnabled()
    {
        return getTelemetryConfig().tracingEnabled;
    }

    /**
     * Gets span sampling.
     *
     * @return the span sampling
     */
    public static Boolean getSpanSampling()
    {
        return getTelemetryConfig().spanSampling;
    }
}
