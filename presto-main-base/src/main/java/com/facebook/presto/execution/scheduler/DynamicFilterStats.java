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

import com.facebook.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import static com.facebook.presto.server.SimpleHttpResponseHandlerStats.IncrementalAverage;

/**
 * Server-wide JMX metrics for dynamic partition pruning.
 *
 * <p>These metrics aggregate across all queries and are exported via JMX
 * for operational monitoring. Per-query metrics are tracked separately
 * via RuntimeMetric constants.
 */
public class DynamicFilterStats
{
    private final CounterStat filterFetchSuccess = new CounterStat();
    private final CounterStat filterFetchFailure = new CounterStat();
    private final CounterStat filtersCollected = new CounterStat();
    private final CounterStat filterRegistrations = new CounterStat();
    private final CounterStat filterCollectionCompleted = new CounterStat();
    private final CounterStat filterCollectionTimedOut = new CounterStat();
    private final CounterStat filtersStored = new CounterStat();
    private final CounterStat fetchersStarted = new CounterStat();
    private final CounterStat filterFlushes = new CounterStat();
    private final IncrementalAverage filterFetchRoundTripMillis = new IncrementalAverage();

    @Managed
    @Nested
    public CounterStat getFilterFetchSuccess()
    {
        return filterFetchSuccess;
    }

    @Managed
    @Nested
    public CounterStat getFilterFetchFailure()
    {
        return filterFetchFailure;
    }

    @Managed
    @Nested
    public CounterStat getFiltersCollected()
    {
        return filtersCollected;
    }

    @Managed
    @Nested
    public CounterStat getFilterRegistrations()
    {
        return filterRegistrations;
    }

    @Managed
    @Nested
    public CounterStat getFilterCollectionCompleted()
    {
        return filterCollectionCompleted;
    }

    @Managed
    @Nested
    public CounterStat getFilterCollectionTimedOut()
    {
        return filterCollectionTimedOut;
    }

    @Managed
    @Nested
    public CounterStat getFiltersStored()
    {
        return filtersStored;
    }

    @Managed
    @Nested
    public CounterStat getFetchersStarted()
    {
        return fetchersStarted;
    }

    @Managed
    @Nested
    public CounterStat getFilterFlushes()
    {
        return filterFlushes;
    }

    @Managed
    public double getFilterFetchRoundTripMillis()
    {
        return filterFetchRoundTripMillis.getAverage();
    }

    @Managed
    public long getFilterFetchRoundTripCount()
    {
        return filterFetchRoundTripMillis.getCount();
    }

    public void recordFilterFetchRoundTripMillis(long millis)
    {
        filterFetchRoundTripMillis.add(millis);
    }
}
