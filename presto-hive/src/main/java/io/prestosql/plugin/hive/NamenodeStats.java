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

import io.airlift.stats.CounterStat;
import io.airlift.stats.TimeStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NamenodeStats
{
    private final CallStats listLocatedStatus = new CallStats();
    private final CallStats remoteIteratorNext = new CallStats();

    @Managed
    @Nested
    public CallStats getListLocatedStatus()
    {
        return listLocatedStatus;
    }

    @Managed
    @Nested
    public CallStats getRemoteIteratorNext()
    {
        return remoteIteratorNext;
    }

    public static class CallStats
    {
        private final TimeStat time = new TimeStat(TimeUnit.MILLISECONDS);
        private final CounterStat totalFailures = new CounterStat();
        private final CounterStat ioExceptions = new CounterStat();

        public TimeStat.BlockTimer time()
        {
            return time.time();
        }

        public void recordException(Exception exception)
        {
            if (exception instanceof IOException) {
                ioExceptions.update(1);
            }
            totalFailures.update(1);
        }

        @Managed
        @Nested
        public CounterStat getTotalFailures()
        {
            return totalFailures;
        }

        @Managed
        @Nested
        public CounterStat getIoExceptions()
        {
            return ioExceptions;
        }

        @Managed
        @Nested
        public TimeStat getTime()
        {
            return time;
        }
    }
}
