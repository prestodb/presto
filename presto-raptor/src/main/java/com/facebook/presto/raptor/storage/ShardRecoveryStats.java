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
package com.facebook.presto.raptor.storage;

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class ShardRecoveryStats
{
    private final CounterStat activeShardRecovery = new CounterStat();
    private final CounterStat backgroundShardRecovery = new CounterStat();
    private final CounterStat shardRecoverySuccess = new CounterStat();
    private final CounterStat shardRecoveryFailure = new CounterStat();
    private final CounterStat shardRecoveryBackupNotFound = new CounterStat();

    private final DistributionStat shardRecoveryShardSizeBytes = new DistributionStat();
    private final DistributionStat shardRecoveryTimeInMilliSeconds = new DistributionStat();
    private final DistributionStat shardRecoveryBytesPerSecond = new DistributionStat();

    private final CounterStat corruptLocalFile = new CounterStat();
    private final CounterStat corruptRecoveredFile = new CounterStat();

    public void incrementBackgroundShardRecovery()
    {
        backgroundShardRecovery.update(1);
    }

    public void incrementActiveShardRecovery()
    {
        activeShardRecovery.update(1);
    }

    public void incrementShardRecoveryBackupNotFound()
    {
        shardRecoveryBackupNotFound.update(1);
    }

    public void incrementShardRecoveryFailure()
    {
        shardRecoveryFailure.update(1);
    }

    public void incrementShardRecoverySuccess()
    {
        shardRecoverySuccess.update(1);
    }

    public void addShardRecoveryDataRate(DataSize rate, DataSize size, Duration duration)
    {
        shardRecoveryBytesPerSecond.add(rate.toBytes());
        shardRecoveryShardSizeBytes.add(size.toBytes());
        shardRecoveryTimeInMilliSeconds.add(duration.toMillis());
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
    public CounterStat getActiveShardRecovery()
    {
        return activeShardRecovery;
    }

    @Managed
    @Nested
    public CounterStat getBackgroundShardRecovery()
    {
        return backgroundShardRecovery;
    }

    @Managed
    @Nested
    public CounterStat getShardRecoverySuccess()
    {
        return shardRecoverySuccess;
    }

    @Managed
    @Nested
    public CounterStat getShardRecoveryFailure()
    {
        return shardRecoveryFailure;
    }

    @Managed
    @Nested
    public CounterStat getShardRecoveryBackupNotFound()
    {
        return shardRecoveryBackupNotFound;
    }

    @Managed
    @Nested
    public DistributionStat getShardRecoveryBytesPerSecond()
    {
        return shardRecoveryBytesPerSecond;
    }

    @Managed
    @Nested
    public DistributionStat getShardRecoveryTimeInMilliSeconds()
    {
        return shardRecoveryTimeInMilliSeconds;
    }

    @Managed
    @Nested
    public DistributionStat getShardRecoveryShardSizeBytes()
    {
        return shardRecoveryShardSizeBytes;
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
