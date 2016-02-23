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

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

public class RemoteTaskStats
{
    private final DistributionStat updateRoundTripMillis = new DistributionStat();
    private final DistributionStat infoRoundTripMillis = new DistributionStat();
    private final DistributionStat statusRoundTripMillis = new DistributionStat();
    private final DistributionStat responseSizeBytes = new DistributionStat();

    private final CounterStat requestSuccess = new CounterStat();
    private final CounterStat requestFailure = new CounterStat();

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

    @Managed
    @Nested
    public DistributionStat getResponseSizeBytes()
    {
        return responseSizeBytes;
    }

    @Managed
    @Nested
    public DistributionStat getStatusRoundTripMillis()
    {
        return statusRoundTripMillis;
    }

    @Managed
    @Nested
    public DistributionStat getUpdateRoundTripMillis()
    {
        return updateRoundTripMillis;
    }

    @Managed
    @Nested
    public DistributionStat getInfoRoundTripMillis()
    {
        return infoRoundTripMillis;
    }

    @Managed
    @Nested
    public void updateSuccess()
    {
        requestSuccess.update(1);
    }

    @Managed
    @Nested
    public void updateFailure()
    {
        requestFailure.update(1);
    }
}
