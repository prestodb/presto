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

import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import static com.facebook.presto.raptor.storage.ShardRecoveryManager.dataRate;

@ThreadSafe
public class StorageManagerStats
{
    private final DistributionStat createParentsBackupMillis = new DistributionStat();
    private final DistributionStat copyToBackupBytesPerSecond = new DistributionStat();
    private final DistributionStat copyToBackupShardSizeBytes = new DistributionStat();
    private final DistributionStat copyToBackupTimeInMilliSeconds = new DistributionStat();

    public void addCreateParentsTime(Duration duration)
    {
        createParentsBackupMillis.add(duration.toMillis());
    }

    public void addCopyShardDataRate(DataSize size, Duration duration)
    {
        DataSize rate = dataRate(size, duration).convertToMostSuccinctDataSize();
        copyToBackupBytesPerSecond.add(Math.round(rate.toBytes()));
        copyToBackupShardSizeBytes.add(size.toBytes());
        copyToBackupTimeInMilliSeconds.add(duration.toMillis());
    }

    @Managed
    @Nested
    public DistributionStat getCreateParentsBackupMillis()
    {
        return createParentsBackupMillis;
    }

    @Managed
    @Nested
    public DistributionStat getCopyToBackupBytesPerSecond()
    {
        return copyToBackupBytesPerSecond;
    }

    @Managed
    @Nested
    public DistributionStat getCopyToBackupShardSizeBytes()
    {
        return copyToBackupShardSizeBytes;
    }

    @Managed
    @Nested
    public DistributionStat getCopyToBackupTimeInMilliSeconds()
    {
        return copyToBackupTimeInMilliSeconds;
    }
}
