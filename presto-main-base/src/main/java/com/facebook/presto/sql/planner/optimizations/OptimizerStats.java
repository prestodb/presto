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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.airlift.stats.TimeDistribution;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class OptimizerStats
{
    private final AtomicLong failures = new AtomicLong();
    private final TimeDistribution time = new TimeDistribution(TimeUnit.MICROSECONDS);

    public void record(long nanos)
    {
        time.add(nanos);
    }

    public void recordFailure()
    {
        failures.incrementAndGet();
    }

    @Managed
    @Nested
    public TimeDistribution getTime()
    {
        return time;
    }

    @Managed
    public long getFailures()
    {
        return failures.get();
    }
}
