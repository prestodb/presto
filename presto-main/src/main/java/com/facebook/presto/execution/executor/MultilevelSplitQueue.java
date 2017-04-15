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

import com.google.common.collect.ImmutableList;
import io.airlift.stats.CounterStat;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class MultilevelSplitQueue
{
    public static final int[] LEVELS = {0, 1, 10, 60, 300};

    @GuardedBy("lock")
    private final List<PriorityQueue<PrioritizedSplitRunner>> levelWaitingSplits;
    private final AtomicLong[] levelMinPriority;

    private final ReentrantLock lock;
    private final Condition notEmpty;

    private final List<CounterStat> selectedLevelCounters;

    public MultilevelSplitQueue()
    {
        this.levelMinPriority = new AtomicLong[LEVELS.length];
        this.levelWaitingSplits = new ArrayList<>(LEVELS.length);
        ImmutableList.Builder<CounterStat> counters = ImmutableList.builder();

        for (int i = 0; i < LEVELS.length; i++) {
            levelMinPriority[i] = new AtomicLong(-1);
            levelWaitingSplits.add(new PriorityQueue<>());
            counters.add(new CounterStat());
        }

        this.selectedLevelCounters = counters.build();
        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
    }

    public void offer(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");

        split.setReady();
        lock.lock();
        try {
            levelWaitingSplits.get(split.getLevel()).offer(split);
            notEmpty.signal();
        }
        finally {
            lock.unlock();
        }
    }

    public PrioritizedSplitRunner take()
            throws InterruptedException
    {
        while (true) {
            lock.lockInterruptibly();
            try {
                PrioritizedSplitRunner result;
                while ((result = pollFirstSplit()) == null) {
                    notEmpty.await();
                }

                if (result.updateLevelPriority()) {
                    offer(result);
                    continue;
                }

                int selectedLevel = result.getLevel();
                levelMinPriority[selectedLevel].set(result.getLevelPriority());
                selectedLevelCounters.get(selectedLevel).update(1);

                return result;
            }
            finally {
                lock.unlock();
            }
        }
    }

    @GuardedBy("lock")
    private PrioritizedSplitRunner pollFirstSplit()
    {
        for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
            PrioritizedSplitRunner split = level.poll();
            if (split != null) {
                return split;
            }
        }

        return null;
    }

    public void remove(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");
        lock.lock();
        try {
            for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
                level.remove(split);
            }
        }
        finally {
            lock.unlock();
        }
    }

    public void removeAll(Collection<PrioritizedSplitRunner> splits)
    {
        lock.lock();
        try {
            for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
                level.removeAll(splits);
            }
        }
        finally {
            lock.unlock();
        }
    }

    public long getLevelMinPriority(int level, long taskThreadUsageNanos)
    {
        levelMinPriority[level].compareAndSet(-1, taskThreadUsageNanos);
        return levelMinPriority[level].get();
    }

    public int size()
    {
        lock.lock();
        try {
            int total = 0;
            for (PriorityQueue<PrioritizedSplitRunner> level : levelWaitingSplits) {
                total += level.size();
            }
            return total;
        }
        finally {
            lock.unlock();
        }
    }

    public List<CounterStat> getSelectedLevelCounters()
    {
        return selectedLevelCounters;
    }

    public static int computeLevel(long threadUsageNanos)
    {
        long seconds = NANOSECONDS.toSeconds(threadUsageNanos);
        for (int i = 0; i < (LEVELS.length - 1); i++) {
            if (seconds < LEVELS[i + 1]) {
                return i;
            }
        }

        return LEVELS.length - 1;
    }
}
