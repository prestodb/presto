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
package com.facebook.presto.server.remotetask;

import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

public class RemoteTaskStats
{
    private final IncrementalAverage updateRoundTripMillis = new IncrementalAverage();
    private final IncrementalAverage infoRoundTripMillis = new IncrementalAverage();
    private final IncrementalAverage statusRoundTripMillis = new IncrementalAverage();
    private final IncrementalAverage responseSizeBytes = new IncrementalAverage();
    private final DistributionStat updateWithPlanSize = new DistributionStat();
    private final DistributionStat updateWithoutPlanSize = new DistributionStat();

    private long requestSuccess;
    private long requestFailure;

    public void statusRoundTripMillis(long roundTripMillis)
    {
        statusRoundTripMillis.add(roundTripMillis);
    }

    public void infoRoundTripMillis(long roundTripMillis)
    {
        infoRoundTripMillis.add(roundTripMillis);
    }

    public void updateRoundTripMillis(long roundTripMillis)
    {
        updateRoundTripMillis.add(roundTripMillis);
    }

    public void responseSize(long responseSizeBytes)
    {
        this.responseSizeBytes.add(responseSizeBytes);
    }

    public void updateSuccess()
    {
        requestSuccess++;
    }

    public void updateFailure()
    {
        requestFailure++;
    }

    public void updateWithPlanSize(long bytes)
    {
        updateWithPlanSize.add(bytes);
    }

    public void updateWithoutPlanSize(long bytes)
    {
        updateWithoutPlanSize.add(bytes);
    }

    @Managed
    public double getResponseSizeBytes()
    {
        return responseSizeBytes.getAverage();
    }

    @Managed
    public double getStatusRoundTripMillis()
    {
        return statusRoundTripMillis.getAverage();
    }

    @Managed
    public long getStatusRoundTripCount()
    {
        return statusRoundTripMillis.getCount();
    }

    @Managed
    public double getUpdateRoundTripMillis()
    {
        return updateRoundTripMillis.getAverage();
    }

    @Managed
    public long getUpdateRoundTripCount()
    {
        return updateRoundTripMillis.getCount();
    }

    @Managed
    public double getInfoRoundTripMillis()
    {
        return infoRoundTripMillis.getAverage();
    }

    @Managed
    public long getInfoRoundTripCount()
    {
        return infoRoundTripMillis.getCount();
    }

    @Managed
    public long getRequestSuccess()
    {
        return requestSuccess;
    }

    @Managed
    public long getRequestFailure()
    {
        return requestFailure;
    }

    @Managed
    @Nested
    public DistributionStat getUpdateWithPlanSize()
    {
        return updateWithPlanSize;
    }

    @Managed
    @Nested
    public DistributionStat getUpdateWithoutPlanSize()
    {
        return updateWithoutPlanSize;
    }

    @ThreadSafe
    private static class IncrementalAverage
    {
        private volatile long count;
        private volatile double average;

        synchronized void add(long value)
        {
            count++;
            average = average + (value - average) / count;
        }

        double getAverage()
        {
            return average;
        }

        long getCount()
        {
            return count;
        }
    }
}
