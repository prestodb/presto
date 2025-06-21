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
 * The type TelemetryConfig to store the values from telemetry-tracing.properties.
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
    private boolean tracingEnabled;
    private boolean spanSampling;

    /**
     * The type TelemetryConfigConstants to store constants.
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
     * Gets the singleton telemetryConfig.
     *
     * @return the telemetry config
     */
    public static TelemetryConfig getTelemetryConfig()
    {   // prevent multiple instance creation
        telemetryConfig = nonNull(telemetryConfig) ? telemetryConfig : new TelemetryConfig();
        return telemetryConfig;
    }

    /**
     * Sets telemetry properties from the input.
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

    public void setSpanSampling(Boolean spanSampling)
    {
        getTelemetryConfig().spanSampling = spanSampling;
    }

    public String getTracingBackendUrl()
    {
        return this.tracingBackendUrl;
    }

    public Integer getMaxExporterBatchSize()
    {
        return this.maxExporterBatchSize;
    }

    public Integer getMaxQueueSize()
    {
        return this.maxQueueSize;
    }

    public Integer getExporterTimeout()
    {
        return this.exporterTimeout;
    }

    public Integer getScheduleDelay()
    {
        return this.scheduleDelay;
    }

    public Double getSamplingRatio()
    {
        return this.samplingRatio;
    }

    public static Boolean getTracingEnabled()
    {
        return getTelemetryConfig().tracingEnabled;
    }

    public static Boolean getSpanSampling()
    {
        return getTelemetryConfig().spanSampling;
    }
}
