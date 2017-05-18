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

import com.facebook.presto.raptor.backup.BackupCopyStats;
import io.airlift.stats.CounterStat;
import io.airlift.units.DataSize;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import java.util.function.Supplier;

@ThreadSafe
public class ShardRecoveryStats
{
    private final CounterStat activeShardRecovery = new CounterStat();
    private final CounterStat backgroundShardRecovery = new CounterStat();

    private final CounterStat shardRecoverySuccess = new CounterStat();
    private final CounterStat shardRecoveryFailure = new CounterStat();
    private final CounterStat shardRecoveryBackupNotFound = new CounterStat();

    private final BackupCopyStats recoverCopyStats = new BackupCopyStats();

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

    public void run(Runnable runnable, Supplier<DataSize> supplier)
    {
        recoverCopyStats.run(runnable, supplier);
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
    public CounterStat getShardRecoveryBackupNotFound()
    {
        return shardRecoveryBackupNotFound;
    }
}
