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
package io.prestosql.server.remotetask;

import com.google.common.util.concurrent.AtomicDouble;
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
    private final DistributionStat updateWithPlanBytes = new DistributionStat();

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

    public void updateWithPlanBytes(long bytes)
    {
        updateWithPlanBytes.add(bytes);
    }

    @Managed
    public double getResponseSizeBytes()
    {
        return responseSizeBytes.get();
    }

    @Managed
    public double getStatusRoundTripMillis()
    {
        return statusRoundTripMillis.get();
    }

    @Managed
    public double getUpdateRoundTripMillis()
    {
        return updateRoundTripMillis.get();
    }

    @Managed
    public double getInfoRoundTripMillis()
    {
        return infoRoundTripMillis.get();
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
    public DistributionStat getUpdateWithPlanBytes()
    {
        return updateWithPlanBytes;
    }

    @ThreadSafe
    private static class IncrementalAverage
    {
        private long count;
        private final AtomicDouble average = new AtomicDouble();

        synchronized void add(long value)
        {
            count++;
            double oldAverage = average.get();
            average.set(oldAverage + ((value - oldAverage) / count));
        }

        double get()
        {
            return average.get();
        }
    }
}
