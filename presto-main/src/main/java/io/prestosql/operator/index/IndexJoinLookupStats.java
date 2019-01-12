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
package io.prestosql.operator.index;

import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class IndexJoinLookupStats
{
    private final CounterStat totalIndexJoinLookups = new CounterStat();
    private final CounterStat successfulIndexJoinLookupsByCacheReset = new CounterStat();
    private final CounterStat successfulIndexJoinLookupsBySingleRequest = new CounterStat();
    private final CounterStat successfulIndexJoinLookupsByLimitedRequest = new CounterStat();
    private final CounterStat streamedIndexJoinLookups = new CounterStat();

    @Managed
    @Nested
    public CounterStat getTotalIndexJoinLookups()
    {
        return totalIndexJoinLookups;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulIndexJoinLookupsByCacheReset()
    {
        return successfulIndexJoinLookupsByCacheReset;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulIndexJoinLookupsBySingleRequest()
    {
        return successfulIndexJoinLookupsBySingleRequest;
    }

    @Managed
    @Nested
    public CounterStat getSuccessfulIndexJoinLookupsByLimitedRequest()
    {
        return successfulIndexJoinLookupsByLimitedRequest;
    }

    @Managed
    @Nested
    public CounterStat getStreamedIndexJoinLookups()
    {
        return streamedIndexJoinLookups;
    }

    public void recordIndexJoinLookup()
    {
        totalIndexJoinLookups.update(1);
    }

    public void recordSuccessfulIndexJoinLookupByCacheReset()
    {
        successfulIndexJoinLookupsByCacheReset.update(1);
    }

    public void recordSuccessfulIndexJoinLookupBySingleRequest()
    {
        successfulIndexJoinLookupsBySingleRequest.update(1);
    }

    public void recordSuccessfulIndexJoinLookupByLimitedRequest()
    {
        successfulIndexJoinLookupsByLimitedRequest.update(1);
    }

    public void recordStreamedIndexJoinLookup()
    {
        streamedIndexJoinLookups.update(1);
    }
}
