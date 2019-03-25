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
package com.facebook.presto.execution.scheduler;

import io.airlift.stats.CounterStat;
import io.airlift.stats.DistributionStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@ThreadSafe
public class SplitSchedulerStats
{
    private final TimeStat sleepTime = new TimeStat(MILLISECONDS);
    private final TimeStat getSplitTime = new TimeStat(MILLISECONDS);
    private final CounterStat waitingForSource = new CounterStat();
    private final CounterStat splitQueuesFull = new CounterStat();
    private final DistributionStat splitsPerIteration = new DistributionStat();

    @Managed
    @Nested
    public TimeStat getSleepTime()
    {
        return sleepTime;
    }

    @Managed
    @Nested
    public TimeStat getGetSplitTime()
    {
        return getSplitTime;
    }

    @Managed
    @Nested
    public DistributionStat getSplitsScheduledPerIteration()
    {
        return splitsPerIteration;
    }

    @Managed
    @Nested
    public CounterStat getWaitingForSource()
    {
        return waitingForSource;
    }

    @Managed
    @Nested
    public CounterStat getSplitQueuesFull()
    {
        return splitQueuesFull;
    }
}
