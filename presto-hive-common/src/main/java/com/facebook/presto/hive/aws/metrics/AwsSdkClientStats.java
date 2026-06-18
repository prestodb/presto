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
package com.facebook.presto.hive.aws.metrics;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import com.google.errorprone.annotations.ThreadSafe;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

import java.time.Duration;

import static java.time.Duration.ZERO;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static software.amazon.awssdk.core.internal.metrics.SdkErrorType.THROTTLING;
import static software.amazon.awssdk.core.metrics.CoreMetric.API_CALL_DURATION;
import static software.amazon.awssdk.core.metrics.CoreMetric.BACKOFF_DELAY_DURATION;
import static software.amazon.awssdk.core.metrics.CoreMetric.ERROR_TYPE;
import static software.amazon.awssdk.core.metrics.CoreMetric.RETRY_COUNT;
import static software.amazon.awssdk.core.metrics.CoreMetric.SERVICE_CALL_DURATION;

/**
 * For reference on AWS SDK v2 Metrics: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics-list.html
 * Metrics Publisher: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics.html
 */
@ThreadSafe
public final class AwsSdkClientStats
{
    private final CounterStat awsRequestCount = new CounterStat();
    private final CounterStat awsRetryCount = new CounterStat();
    private final CounterStat awsThrottleExceptions = new CounterStat();
    private final TimeStat awsServiceCallDuration = new TimeStat(MILLISECONDS);
    private final TimeStat awsApiCallDuration = new TimeStat(MILLISECONDS);
    private final TimeStat awsBackoffDelayDuration = new TimeStat(MILLISECONDS);

    @Managed
    @Nested
    public CounterStat getAwsRequestCount()
    {
        return awsRequestCount;
    }

    @Managed
    @Nested
    public CounterStat getAwsRetryCount()
    {
        return awsRetryCount;
    }

    @Managed
    @Nested
    public CounterStat getAwsThrottleExceptions()
    {
        return awsThrottleExceptions;
    }

    @Managed
    @Nested
    public TimeStat getAwsServiceCallDuration()
    {
        return awsServiceCallDuration;
    }

    @Managed
    @Nested
    public TimeStat getAwsApiCallDuration()
    {
        return awsApiCallDuration;
    }

    @Managed
    @Nested
    public TimeStat getAwsBackoffDelayDuration()
    {
        return awsBackoffDelayDuration;
    }

    public AwsSdkClientRequestMetricsPublisher newRequestMetricsPublisher()
    {
        return new AwsSdkClientRequestMetricsPublisher(this);
    }

    public static class AwsSdkClientRequestMetricsPublisher
            implements MetricPublisher
    {
        private final AwsSdkClientStats stats;

        protected AwsSdkClientRequestMetricsPublisher(AwsSdkClientStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
        }

        @Override
        public void publish(MetricCollection metricCollection)
        {
            long requestCount = metricCollection.metricValues(RETRY_COUNT)
                    .stream()
                    .map(i -> i + 1)
                    .reduce(Integer::sum).orElse(0);

            stats.awsRequestCount.update(requestCount);

            long retryCount = metricCollection.metricValues(RETRY_COUNT)
                    .stream()
                    .reduce(Integer::sum).orElse(0);

            stats.awsRetryCount.update(retryCount);

            long throttleExceptions = metricCollection
                    .childrenWithName("ApiCallAttempt")
                    .flatMap(mc -> mc.metricValues(ERROR_TYPE).stream())
                    .filter(s -> s.equals(THROTTLING.toString()))
                    .count();

            stats.awsThrottleExceptions.update(throttleExceptions);

            Duration serviceCallDuration = metricCollection
                    .childrenWithName("ApiCallAttempt")
                    .flatMap(mc -> mc.metricValues(SERVICE_CALL_DURATION).stream())
                    .reduce(Duration::plus).orElse(ZERO);

            stats.awsServiceCallDuration.add(serviceCallDuration.toMillis(), MILLISECONDS);

            Duration apiCallDuration = metricCollection
                    .metricValues(API_CALL_DURATION)
                    .stream().reduce(Duration::plus).orElse(ZERO);

            stats.awsApiCallDuration.add(apiCallDuration.toMillis(), MILLISECONDS);

            Duration backoffDelayDuration = metricCollection
                    .childrenWithName("ApiCallAttempt")
                    .flatMap(mc -> mc.metricValues(BACKOFF_DELAY_DURATION).stream())
                    .reduce(Duration::plus).orElse(ZERO);

            stats.awsBackoffDelayDuration.add(backoffDelayDuration.toMillis(), MILLISECONDS);
        }

        @Override
        public void close()
        {
        }
    }
}
