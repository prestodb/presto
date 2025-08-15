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
package com.facebook.presto.execution.scheduler.nodeSelection;

import com.facebook.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class NodeSelectionStats
{
    private final CounterStat primaryPreferredNodeSelectedCount = new CounterStat();
    private final CounterStat nonPrimaryPreferredNodeSelectedCount = new CounterStat();
    private final CounterStat nonPreferredNodeSelectedCount = new CounterStat();

    private final CounterStat bucketedPreferredNodeSelectedCount = new CounterStat();
    private final CounterStat bucketedNonPreferredNodeSelectedCount = new CounterStat();
    private final CounterStat bucketedNonAliveNodeReplacedCount = new CounterStat();

    private final CounterStat preferredNonAliveNodeSkippedCount = new CounterStat();

    public void incrementPrimaryPreferredNodeSelectedCount()
    {
        primaryPreferredNodeSelectedCount.update(1);
    }

    public void incrementNonPrimaryPreferredNodeSelectedCount()
    {
        nonPrimaryPreferredNodeSelectedCount.update(1);
    }

    public void incrementNonPreferredNodeSelectedCount()
    {
        nonPreferredNodeSelectedCount.update(1);
    }

    public void incrementBucketedPreferredNodeSelectedCount()
    {
        bucketedPreferredNodeSelectedCount.update(1);
    }

    public void incrementBucketedNonPreferredNodeSelectedCount()
    {
        bucketedNonPreferredNodeSelectedCount.update(1);
    }

    public void incrementBucketedNonAliveNodeReplacedCount()
    {
        bucketedNonAliveNodeReplacedCount.update(1);
    }

    public void incrementPreferredNonAliveNodeSkippedCount()
    {
        preferredNonAliveNodeSkippedCount.update(1);
    }

    @Managed
    @Nested
    public CounterStat getPrimaryPreferredNodeSelectedCount()
    {
        return primaryPreferredNodeSelectedCount;
    }

    @Managed
    @Nested
    public CounterStat getNonPrimaryPreferredNodeSelectedCount()
    {
        return nonPrimaryPreferredNodeSelectedCount;
    }

    @Managed
    @Nested
    public CounterStat getNonPreferredNodeSelectedCount()
    {
        return nonPreferredNodeSelectedCount;
    }

    @Managed
    @Nested
    public CounterStat getBucketedPreferredNodeSelectedCount()
    {
        return bucketedPreferredNodeSelectedCount;
    }

    @Managed
    @Nested
    public CounterStat getBucketedNonPreferredNodeSelectedCount()
    {
        return bucketedNonPreferredNodeSelectedCount;
    }

    @Managed
    @Nested
    public CounterStat getPreferredNonAliveNodeSkippedCount()
    {
        return preferredNonAliveNodeSkippedCount;
    }

    @Managed
    @Nested
    public CounterStat getBucketedNonAliveNodeReplacedCount()
    {
        return bucketedNonAliveNodeReplacedCount;
    }
}
