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
package com.facebook.presto.hive.s3;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.Duration;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;
import software.amazon.awssdk.metrics.SdkMetric;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * AWS SDK v2 compatible metric collector for Presto S3 FileSystem operations.
 * This class implements MetricPublisher to collect and report metrics from AWS SDK v2 operations.
 */
public class PrestoS3FileSystemMetricCollector
        implements MetricPublisher
{
    private static final Logger log = Logger.get(PrestoS3FileSystemMetricCollector.class);

    private final PrestoS3FileSystemStats stats;
    private volatile boolean closed;

    public PrestoS3FileSystemMetricCollector(PrestoS3FileSystemStats stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void publish(MetricCollection metricCollection)
    {
        if (closed) {
            return;
        }

        try {
            // Process all metrics in the collection
            for (MetricRecord<?> record : metricCollection) {
                processMetricRecord(record);
            }
        }
        catch (Exception e) {
            log.debug("Error processing metrics: %s", e.getMessage());
        }
    }

    @Override
    public void close()
    {
        closed = true;
    }

    private void processMetricRecord(MetricRecord<?> record)
    {
        String metricName = record.metric().name();
        Object value = record.value();

        // Handle core AWS SDK metrics
        if (CoreMetric.API_CALL_DURATION.equals(record.metric())) {
            if (value instanceof java.time.Duration) {
                Duration duration = Duration.succinctDuration(((java.time.Duration) value).toMillis(), TimeUnit.MILLISECONDS);
                recordClientExecutionTime(duration);
            }
        }
        else if (CoreMetric.MARSHALLING_DURATION.equals(record.metric())) {
            if (value instanceof java.time.Duration) {
                Duration duration = Duration.succinctDuration(((java.time.Duration) value).toMillis(), TimeUnit.MILLISECONDS);
                recordHttpRequestTime(duration);
            }
        }
        else if (CoreMetric.SERVICE_CALL_DURATION.equals(record.metric())) {
            if (value instanceof java.time.Duration) {
                Duration duration = Duration.succinctDuration(((java.time.Duration) value).toMillis(), TimeUnit.MILLISECONDS);
                recordHttpRequestTime(duration);
            }
        }
        else if (CoreMetric.RETRY_COUNT.equals(record.metric())) {
            if (value instanceof Number) {
                recordRetryCount(((Number) value).longValue());
            }
        }
        else if (HttpMetric.HTTP_STATUS_CODE.equals(record.metric())) {
            if (value instanceof Number) {
                int statusCode = ((Number) value).intValue();
                if (statusCode == 429 || statusCode == 503) {
                    recordThrottleExceptionCount(1);
                }
            }
        }
        else if (HttpMetric.HTTP_CLIENT_NAME.equals(record.metric())) {
            // Track request count
            recordRequestCount(1);
        }

        // Handle specific metric names for additional tracking
        switch (metricName) {
            case "ApiCall":
            case "HttpRequest":
                recordRequestCount(1);
                break;
            case "RetryDelay":
                if (value instanceof java.time.Duration) {
                    Duration duration = Duration.succinctDuration(((java.time.Duration) value).toMillis(), TimeUnit.MILLISECONDS);
                    recordRetryPauseTime(duration);
                }
                break;
            case "Exception":
                // Handle various exceptions
                if (value instanceof S3Exception) {
                    S3Exception s3Exception = (S3Exception) value;
                    if (s3Exception.statusCode() == 429 || s3Exception.statusCode() == 503) {
                        recordThrottleExceptionCount(1);
                    }
                }
                break;
        }
    }

    protected void recordRequestCount(long count)
    {
        stats.updateAwsRequestCount(count);
    }

    protected void recordRetryCount(long count)
    {
        stats.updateAwsRetryCount(count);
    }

    protected void recordThrottleExceptionCount(long count)
    {
        stats.updateAwsThrottleExceptionsCount(count);
    }

    protected void recordHttpRequestTime(Duration duration)
    {
        stats.addAwsRequestTime(duration);
    }

    protected void recordClientExecutionTime(Duration duration)
    {
        stats.addAwsClientExecuteTime(duration);
    }

    protected void recordRetryPauseTime(Duration duration)
    {
        stats.addAwsClientRetryPauseTime(duration);
    }

    /**
     * Utility method to convert java.time.Duration to Airlift Duration
     */
    private static Duration convertDuration(java.time.Duration javaDuration)
    {
        return Duration.succinctDuration(javaDuration.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Check if this collector is handling the right type of metrics
     */
    public boolean isValidMetric(MetricRecord<?> record)
    {
        SdkMetric<?> metric = record.metric();

        return metric.equals(CoreMetric.API_CALL_DURATION)
                || metric.equals(CoreMetric.MARSHALLING_DURATION)
                || metric.equals(CoreMetric.SERVICE_CALL_DURATION)
                || metric.equals(CoreMetric.RETRY_COUNT)
                || metric.equals(HttpMetric.HTTP_STATUS_CODE)
                || metric.equals(HttpMetric.HTTP_CLIENT_NAME);
    }
}
