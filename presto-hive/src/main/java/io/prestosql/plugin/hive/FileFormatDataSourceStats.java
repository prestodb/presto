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
package io.prestosql.plugin.hive;

import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class FileFormatDataSourceStats
{
    private final DistributionStat readBytes = new DistributionStat();
    private final DistributionStat maxCombinedBytesPerRow = new DistributionStat();
    private final TimeStat time0Bto100KB = new TimeStat(MILLISECONDS);
    private final TimeStat time100KBto1MB = new TimeStat(MILLISECONDS);
    private final TimeStat time1MBto10MB = new TimeStat(MILLISECONDS);
    private final TimeStat time10MBPlus = new TimeStat(MILLISECONDS);

    @Managed
    @Nested
    public DistributionStat getReadBytes()
    {
        return readBytes;
    }

    @Managed
    @Nested
    public DistributionStat getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    @Managed
    @Nested
    public TimeStat get0Bto100KB()
    {
        return time0Bto100KB;
    }

    @Managed
    @Nested
    public TimeStat get100KBto1MB()
    {
        return time100KBto1MB;
    }

    @Managed
    @Nested
    public TimeStat get1MBto10MB()
    {
        return time1MBto10MB;
    }

    @Managed
    @Nested
    public TimeStat get10MBPlus()
    {
        return time10MBPlus;
    }

    public void readDataBytesPerSecond(long bytes, long nanos)
    {
        readBytes.add(bytes);
        if (bytes < 100 * 1024) {
            time0Bto100KB.add(nanos, NANOSECONDS);
        }
        else if (bytes < 1024 * 1024) {
            time100KBto1MB.add(nanos, NANOSECONDS);
        }
        else if (bytes < 10 * 1024 * 1024) {
            time1MBto10MB.add(nanos, NANOSECONDS);
        }
        else {
            time10MBPlus.add(nanos, NANOSECONDS);
        }
    }

    public void addMaxCombinedBytesPerRow(long bytes)
    {
        maxCombinedBytesPerRow.add(bytes);
    }
}
