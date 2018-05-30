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
package com.facebook.presto.execution.executor;

import com.facebook.presto.execution.SplitRunner;
import com.google.common.base.Ticker;
import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;

import static com.facebook.presto.execution.executor.MultilevelSplitQueue.LEVEL_THRESHOLD_SECONDS;

public class LegacyPrioritizedSplitRunner
        extends PrioritizedSplitRunner
{
    public LegacyPrioritizedSplitRunner(TaskHandle taskHandle, SplitRunner split, Ticker ticker, CounterStat globalCpuTimeMicros, CounterStat globalScheduledTimeMicros, TimeStat blockedQuantaWallTime, TimeStat unblockedQuantaWallTime)
    {
        super(taskHandle, split, ticker, globalCpuTimeMicros, globalScheduledTimeMicros, blockedQuantaWallTime, unblockedQuantaWallTime);
    }

    @Override
    public int compareTo(PrioritizedSplitRunner o)
    {
        int level = priority.get().getLevel();

        int result = 0;
        if (level == LEVEL_THRESHOLD_SECONDS.length - 1) {
            result = Long.compare(lastRun.get(), o.lastRun.get());
        }

        if (result != 0) {
            return result;
        }

        return super.compareTo(o);
    }
}
