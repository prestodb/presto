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

package com.facebook.presto.raptor.backup;

import io.airlift.stats.DistributionStat;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.succinctDataSize;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class BackupCopyStats
        extends BackupOperationStats
{
    private final DistributionStat copyBytesPerSecond = new DistributionStat();
    private final DistributionStat copyShardSizeBytes = new DistributionStat();
    private final DistributionStat copyTimeInMilliSeconds = new DistributionStat();

    public void run(Runnable runnable, Supplier<DataSize> copiedDataSupplier)
    {
        run(() -> {
            long startedNanos = System.nanoTime();
            runnable.run();

            Duration copyTime = new Duration(System.nanoTime() - startedNanos, NANOSECONDS);
            DataSize copiedSize = copiedDataSupplier.get();

            DataSize rate = dataRate(copiedSize, copyTime).convertToMostSuccinctDataSize();
            copyBytesPerSecond.add(Math.round(rate.toBytes()));
            copyShardSizeBytes.add(copiedSize.toBytes());
            copyTimeInMilliSeconds.add(copyTime.toMillis());

            return null;
        });
    }

    @Managed
    @Nested
    public DistributionStat getCopyBytesPerSecond()
    {
        return copyBytesPerSecond;
    }

    @Managed
    @Nested
    public DistributionStat getCopyShardSizeBytes()
    {
        return copyShardSizeBytes;
    }

    @Managed
    @Nested
    public DistributionStat getCopyTimeInMilliSeconds()
    {
        return copyTimeInMilliSeconds;
    }

    public static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }
        return succinctDataSize(rate, BYTE);
    }
}
