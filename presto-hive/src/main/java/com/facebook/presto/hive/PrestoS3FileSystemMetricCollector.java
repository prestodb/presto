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
package com.facebook.presto.hive;

import com.amazonaws.Request;
import com.amazonaws.Response;
import com.amazonaws.metrics.RequestMetricCollector;
import com.amazonaws.util.AWSRequestMetrics;
import com.amazonaws.util.TimingInfo;

import static com.amazonaws.util.AWSRequestMetrics.Field;

public class PrestoS3FileSystemMetricCollector
        extends RequestMetricCollector
{
    private PrestoS3FileSystemStats fileSystemStats;

    public PrestoS3FileSystemMetricCollector(PrestoS3FileSystemStats fileSystemStats)
    {
        this.fileSystemStats = fileSystemStats;
    }

    @Override
    public void collectMetrics(Request<?> request, Response<?> response)
    {
        AWSRequestMetrics metrics = request.getAWSRequestMetrics();

        final TimingInfo timingInfo = metrics.getTimingInfo();
        Number requestCounts  = timingInfo.getCounter(Field.RequestCount.name());
        Number retryCounts  = timingInfo.getCounter(Field.HttpClientRetryCount.name());
        Number throttleExceptions  = timingInfo.getCounter(Field.ThrottleException.name());

        if (requestCounts != null) {
            fileSystemStats.updateAwsRequestCount(requestCounts.longValue());
        }

        if (retryCounts != null) {
            fileSystemStats.updateAwsRetryCount(retryCounts.longValue());
        }

        if (throttleExceptions != null) {
            fileSystemStats.updateAwsThrottleExceptionsCount(throttleExceptions.longValue());
        }
    }
}
