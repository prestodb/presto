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
package com.facebook.presto.raptorx.chunkstore;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import static com.facebook.presto.raptorx.util.DataRate.dataRate;

@ThreadSafe
public class ChunkStoreStats
{
    private final DistributionStat copyToStoreBytesPerSecond = new DistributionStat();
    private final DistributionStat copyToStoreChunkSizeBytes = new DistributionStat();
    private final DistributionStat copyToStoreTimeInMilliSeconds = new DistributionStat();
    private final DistributionStat queuedTimeMilliSeconds = new DistributionStat();

    private final CounterStat putSuccess = new CounterStat();
    private final CounterStat putFailure = new CounterStat();

    public void addCopyChunkDataRate(DataSize size, Duration duration)
    {
        copyToStoreBytesPerSecond.add(dataRate(size, duration).toBytes());
        copyToStoreChunkSizeBytes.add(size.toBytes());
        copyToStoreTimeInMilliSeconds.add(duration.toMillis());
    }

    public void addQueuedTime(Duration queuedTime)
    {
        queuedTimeMilliSeconds.add(queuedTime.toMillis());
    }

    public void incrementPutSuccess()
    {
        putSuccess.update(1);
    }

    public void incrementPutFailure()
    {
        putFailure.update(1);
    }

    @Managed
    @Nested
    public DistributionStat getCopyToStoreBytesPerSecond()
    {
        return copyToStoreBytesPerSecond;
    }

    @Managed
    @Nested
    public DistributionStat getCopyToStoreChunkSizeBytes()
    {
        return copyToStoreChunkSizeBytes;
    }

    @Managed
    @Nested
    public DistributionStat getCopyToStoreTimeInMilliSeconds()
    {
        return copyToStoreTimeInMilliSeconds;
    }

    @Managed
    @Nested
    public DistributionStat getQueuedTimeMilliSeconds()
    {
        return queuedTimeMilliSeconds;
    }

    @Managed
    @Nested
    public CounterStat getPutSuccess()
    {
        return putSuccess;
    }

    @Managed
    @Nested
    public CounterStat getPutFailure()
    {
        return putFailure;
    }
}
