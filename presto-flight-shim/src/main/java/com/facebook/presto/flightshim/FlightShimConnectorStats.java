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
package com.facebook.presto.flightshim;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FlightShimConnectorStats
{
    private final String connectorId;
    private final AtomicInteger activeStreams = new AtomicInteger();
    private final CounterStat streamsCompleted = new CounterStat();
    private final CounterStat streamErrors = new CounterStat();
    private final CounterStat rowsShipped = new CounterStat();
    private final CounterStat batchesShipped = new CounterStat();
    private final TimeStat streamLatency = new TimeStat(MILLISECONDS);

    public FlightShimConnectorStats(String connectorId)
    {
        this.connectorId = connectorId;
    }

    public void recordStreamStarted()
    {
        activeStreams.incrementAndGet();
    }

    public void recordStreamFinished()
    {
        activeStreams.decrementAndGet();
    }

    public void recordStreamCompleted(long streamLatencyNanos)
    {
        streamsCompleted.update(1);
        streamLatency.add(NANOSECONDS.toMillis(streamLatencyNanos), MILLISECONDS);
    }

    public void recordStreamError(long streamLatencyNanos)
    {
        streamErrors.update(1);
        streamLatency.add(NANOSECONDS.toMillis(streamLatencyNanos), MILLISECONDS);
    }

    public void recordBatchShipped(long rows)
    {
        batchesShipped.update(1);
        rowsShipped.update(rows);
    }

    @Managed
    public String getConnectorId()
    {
        return connectorId;
    }

    @Managed
    public int getActiveStreams()
    {
        return activeStreams.get();
    }

    @Managed
    public long getStreamsCompleted()
    {
        return streamsCompleted.getTotalCount();
    }

    @Managed
    public long getStreamErrors()
    {
        return streamErrors.getTotalCount();
    }

    @Managed
    public long getRowsShipped()
    {
        return rowsShipped.getTotalCount();
    }

    @Managed
    public long getBatchesShipped()
    {
        return batchesShipped.getTotalCount();
    }

    @Managed
    @Nested
    public TimeStat getStreamLatency()
    {
        return streamLatency;
    }
}
