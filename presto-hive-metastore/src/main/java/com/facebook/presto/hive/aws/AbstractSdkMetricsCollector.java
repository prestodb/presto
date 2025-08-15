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
package com.facebook.presto.hive.aws;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.util.AWSRequestMetrics;
import com.amazonaws.util.TimingInfo;
import io.airlift.units.Duration;

import java.util.List;
import java.util.function.Consumer;

import static com.amazonaws.util.AWSRequestMetrics.Field.ClientExecuteTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpClientRetryCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.HttpRequestTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.RequestCount;
import static com.amazonaws.util.AWSRequestMetrics.Field.RetryPauseTime;
import static com.amazonaws.util.AWSRequestMetrics.Field.ThrottleException;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public abstract class AbstractSdkMetricsCollector
        extends RequestMetricCollector
{
    @Override
    public final void collectMetrics(Request<?> request, Response<?> response)
    {
        TimingInfo timingInfo = request.getAWSRequestMetrics().getTimingInfo();

        Number requestCounts = timingInfo.getCounter(RequestCount.name());
        if (requestCounts != null) {
            recordRequestCount(requestCounts.longValue());
        }

        Number retryCounts = timingInfo.getCounter(HttpClientRetryCount.name());
        if (retryCounts != null) {
            recordRetryCount(retryCounts.longValue());
        }

        Number throttleExceptions = timingInfo.getCounter(ThrottleException.name());
        if (throttleExceptions != null) {
            recordThrottleExceptionCount(throttleExceptions.longValue());
        }

        recordSubTimingDurations(timingInfo, HttpRequestTime, this::recordHttpRequestTime);
        recordSubTimingDurations(timingInfo, ClientExecuteTime, this::recordClientExecutionTime);
        recordSubTimingDurations(timingInfo, RetryPauseTime, this::recordRetryPauseTime);
    }

    protected abstract void recordRequestCount(long count);

    protected abstract void recordRetryCount(long count);

    protected abstract void recordThrottleExceptionCount(long count);

    protected abstract void recordHttpRequestTime(Duration duration);

    protected abstract void recordClientExecutionTime(Duration duration);

    protected abstract void recordRetryPauseTime(Duration duration);

    private static void recordSubTimingDurations(TimingInfo timingInfo, AWSRequestMetrics.Field field, Consumer<Duration> consumer)
    {
        List<TimingInfo> subTimings = timingInfo.getAllSubMeasurements(field.name());
        if (subTimings != null) {
            for (TimingInfo subTiming : subTimings) {
                Double millis = subTiming.getTimeTakenMillisIfKnown();
                if (millis != null) {
                    consumer.accept(new Duration(millis, MILLISECONDS));
                }
            }
        }
    }
}
