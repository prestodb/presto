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
package com.facebook.presto.pinot;

import com.facebook.airlift.http.client.StringResponseHandler.StringResponse;
import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.DistributionStat;
import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.pinot.PinotUtils.isValidPinotHttpResponseCode;

@ThreadSafe
public class PinotMetricsStats
{
    private final TimeStat time = new TimeStat(TimeUnit.MILLISECONDS);
    private final CounterStat requests = new CounterStat();
    private final CounterStat errorRequests = new CounterStat();
    private DistributionStat responseSize;

    public PinotMetricsStats(boolean withResponse)
    {
        if (withResponse) {
            responseSize = new DistributionStat();
        }
    }

    public void record(StringResponse response, long duration, TimeUnit timeUnit)
    {
        time.add(duration, timeUnit);
        requests.update(1);
        if (isValidPinotHttpResponseCode(response.getStatusCode())) {
            if (responseSize != null) {
                responseSize.add(response.getBody().length());
            }
        }
        else {
            errorRequests.update(1);
        }
    }

    @Managed
    @Nested
    public TimeStat getTime()
    {
        return time;
    }

    @Managed
    @Nested
    public CounterStat getRequests()
    {
        return requests;
    }

    @Managed
    @Nested
    public CounterStat getErrorRequests()
    {
        return errorRequests;
    }

    @Managed
    @Nested
    public DistributionStat getResponseSize()
    {
        return responseSize;
    }
}
