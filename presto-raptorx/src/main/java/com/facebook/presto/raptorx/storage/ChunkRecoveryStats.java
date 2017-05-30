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
package com.facebook.presto.raptorx.storage;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ChunkRecoveryStats
{
    private final CounterStat activeChunkRecovery = new CounterStat();
    private final CounterStat backgroundChunkRecovery = new CounterStat();
    private final CounterStat chunkRecoverySuccess = new CounterStat();
    private final CounterStat chunkRecoveryFailure = new CounterStat();
    private final CounterStat chunkRecoveryNotFound = new CounterStat();

    private final DistributionStat chunkRecoveryChunkSizeBytes = new DistributionStat();
    private final DistributionStat chunkRecoveryTimeInMilliSeconds = new DistributionStat();
    private final DistributionStat chunkRecoveryBytesPerSecond = new DistributionStat();

    private final CounterStat corruptLocalFile = new CounterStat();
    private final CounterStat corruptRecoveredFile = new CounterStat();

    public void incrementBackgroundChunkRecovery()
    {
        backgroundChunkRecovery.update(1);
    }

    public void incrementActiveChunkRecovery()
    {
        activeChunkRecovery.update(1);
    }

    public void incrementChunkRecoveryNotFound()
    {
        chunkRecoveryNotFound.update(1);
    }

    public void incrementChunkRecoveryFailure()
    {
        chunkRecoveryFailure.update(1);
    }

    public void incrementChunkRecoverySuccess()
    {
        chunkRecoverySuccess.update(1);
    }

    public void addChunkRecoveryDataRate(DataSize rate, DataSize size, Duration duration)
    {
        chunkRecoveryBytesPerSecond.add(Math.round(rate.toBytes()));
        chunkRecoveryChunkSizeBytes.add(size.toBytes());
        chunkRecoveryTimeInMilliSeconds.add(duration.toMillis());
    }

    public void incrementCorruptLocalFile()
    {
        corruptLocalFile.update(1);
    }

    public void incrementCorruptRecoveredFile()
    {
        corruptRecoveredFile.update(1);
    }

    @Managed
    @Nested
    public CounterStat getActiveChunkRecovery()
    {
        return activeChunkRecovery;
    }

    @Managed
    @Nested
    public CounterStat getBackgroundChunkRecovery()
    {
        return backgroundChunkRecovery;
    }

    @Managed
    @Nested
    public CounterStat getChunkRecoverySuccess()
    {
        return chunkRecoverySuccess;
    }

    @Managed
    @Nested
    public CounterStat getChunkRecoveryFailure()
    {
        return chunkRecoveryFailure;
    }

    @Managed
    @Nested
    public CounterStat getChunkRecoveryNotFound()
    {
        return chunkRecoveryNotFound;
    }

    @Managed
    @Nested
    public DistributionStat getChunkRecoveryBytesPerSecond()
    {
        return chunkRecoveryBytesPerSecond;
    }

    @Managed
    @Nested
    public DistributionStat getChunkRecoveryTimeInMilliSeconds()
    {
        return chunkRecoveryTimeInMilliSeconds;
    }

    @Managed
    @Nested
    public DistributionStat getChunkRecoveryChunkSizeBytes()
    {
        return chunkRecoveryChunkSizeBytes;
    }

    @Managed
    @Nested
    public CounterStat getCorruptLocalFile()
    {
        return corruptLocalFile;
    }

    @Managed
    @Nested
    public CounterStat getCorruptRecoveredFile()
    {
        return corruptRecoveredFile;
    }
}
