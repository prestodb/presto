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

import com.amazonaws.AbortedException;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.net.SocketException;
import java.net.SocketTimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrestoS3FileSystemStats
{
    private final CounterStat activeConnections = new CounterStat();
    private final CounterStat startedUploads = new CounterStat();
    private final CounterStat failedUploads = new CounterStat();
    private final CounterStat successfulUploads = new CounterStat();
    private final CounterStat metadataCalls = new CounterStat();
    private final CounterStat listStatusCalls = new CounterStat();
    private final CounterStat listLocatedStatusCalls = new CounterStat();
    private final CounterStat listObjectsCalls = new CounterStat();
    private final CounterStat otherReadErrors = new CounterStat();
    private final CounterStat awsAbortedExceptions = new CounterStat();
    private final CounterStat socketExceptions = new CounterStat();
    private final CounterStat socketTimeoutExceptions = new CounterStat();
    private final CounterStat getObjectErrors = new CounterStat();
    private final CounterStat getMetadataErrors = new CounterStat();
    private final CounterStat getObjectRetries = new CounterStat();
    private final CounterStat getMetadataRetries = new CounterStat();
    private final CounterStat readRetries = new CounterStat();

    // see AWSRequestMetrics
    private final CounterStat awsRequestCount = new CounterStat();
    private final CounterStat awsRetryCount = new CounterStat();
    private final CounterStat awsThrottleExceptions = new CounterStat();
    private final TimeStat awsRequestTime = new TimeStat(MILLISECONDS);

    @Managed
    @Nested
    public CounterStat getActiveConnections()
    {
        return activeConnections;
    }

    @Managed
    @Nested
    public CounterStat getStartedUploads()
    {
        return startedUploads;
    }

    @Managed
    @Nested
    public CounterStat getFailedUploads()
    {
        return failedUploads;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulUploads()
    {
        return successfulUploads;
    }

    @Managed
    @Nested
    public CounterStat getMetadataCalls()
    {
        return metadataCalls;
    }

    @Managed
    @Nested
    public CounterStat getListStatusCalls()
    {
        return listStatusCalls;
    }

    @Managed
    @Nested
    public CounterStat getListLocatedStatusCalls()
    {
        return listLocatedStatusCalls;
    }

    @Managed
    @Nested
    public CounterStat getListObjectsCalls()
    {
        return listObjectsCalls;
    }

    @Managed
    @Nested
    public CounterStat getGetObjectErrors()
    {
        return getObjectErrors;
    }

    @Managed
    @Nested
    public CounterStat getGetMetadataErrors()
    {
        return getMetadataErrors;
    }

    @Managed
    @Nested
    public CounterStat getOtherReadErrors()
    {
        return otherReadErrors;
    }

    @Managed
    @Nested
    public CounterStat getSocketExceptions()
    {
        return socketExceptions;
    }

    @Managed
    @Nested
    public CounterStat getSocketTimeoutExceptions()
    {
        return socketTimeoutExceptions;
    }

    @Managed
    @Nested
    public CounterStat getAwsAbortedExceptions()
    {
        return awsAbortedExceptions;
    }

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
    public TimeStat getAwsRequestTime()
    {
        return awsRequestTime;
    }

    @Managed
    @Nested
    public CounterStat getGetObjectRetries()
    {
        return getObjectRetries;
    }

    @Managed
    @Nested
    public CounterStat getGetMetadataRetries()
    {
        return getMetadataRetries;
    }

    @Managed
    @Nested
    public CounterStat getReadRetries()
    {
        return readRetries;
    }

    public void connectionOpened()
    {
        activeConnections.update(1);
    }

    public void connectionReleased()
    {
        activeConnections.update(-1);
    }

    public void uploadStarted()
    {
        startedUploads.update(1);
    }

    public void uploadFailed()
    {
        failedUploads.update(1);
    }

    public void uploadSuccessful()
    {
        successfulUploads.update(1);
    }

    public void newMetadataCall()
    {
        metadataCalls.update(1);
    }

    public void newListStatusCall()
    {
        listStatusCalls.update(1);
    }

    public void newListLocatedStatusCall()
    {
        listLocatedStatusCalls.update(1);
    }

    public void newListObjectsCall()
    {
        listObjectsCalls.update(1);
    }

    public void newReadError(Exception e)
    {
        if (e instanceof SocketException) {
            socketExceptions.update(1);
        }
        else if (e instanceof SocketTimeoutException) {
            socketTimeoutExceptions.update(1);
        }
        else if (e instanceof AbortedException) {
            awsAbortedExceptions.update(1);
        }
        else {
            otherReadErrors.update(1);
        }
    }

    public void newGetObjectError()
    {
        getObjectErrors.update(1);
    }

    public void newGetMetadataError()
    {
        getMetadataErrors.update(1);
    }

    public void updateAwsRequestCount(long requestCount)
    {
        awsRequestCount.update(requestCount);
    }

    public void updateAwsRetryCount(long retryCount)
    {
        awsRetryCount.update(retryCount);
    }

    public void updateAwsThrottleExceptionsCount(long throttleExceptionsCount)
    {
        awsThrottleExceptions.update(throttleExceptionsCount);
    }

    public void addAwsRequestTime(Duration duration)
    {
        awsRequestTime.add(duration);
    }

    public void newGetObjectRetry()
    {
        getObjectRetries.update(1);
    }

    public void newGetMetadataRetry()
    {
        getMetadataRetries.update(1);
    }

    public void newReadRetry()
    {
        readRetries.update(1);
    }
}
