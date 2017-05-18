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

import io.airlift.stats.CounterStat;
import net.jcip.annotations.ThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class MultilevelSplitQueue
{
    public static final int[] LEVELS = {0, 1, 10, 60, 300};

    private final List<PriorityQueue<PrioritizedSplitRunner>> levelWaitingSplits;
    private final AtomicLong[] levelMinPriority;

    private final ReentrantLock lock;
    private final Condition notEmpty;

    private final List<CounterStat> selectedLevelCounters = Arrays.stream(LEVELS).mapToObj(t -> new CounterStat()).collect(toImmutableList());

    public MultilevelSplitQueue()
    {
        this.levelMinPriority = new AtomicLong[LEVELS.length];
        this.levelWaitingSplits = new ArrayList<>();

        for (int i = 0; i < LEVELS.length; i++) {
            levelMinPriority[i] = new AtomicLong(-1);
            levelWaitingSplits.add(new PriorityQueue<>());
        }

        this.lock = new ReentrantLock();
        this.notEmpty = lock.newCondition();
    }

    public void offer(PrioritizedSplitRunner split)
    {
        lock.lock();
        split.setReady();
        int level = split.getLevel();
        synchronized (levelWaitingSplits.get(level)) {
            levelWaitingSplits.get(level).offer(split);
        }
        notEmpty.signal();
        lock.unlock();
    }

    public PrioritizedSplitRunner take()
            throws InterruptedException
    {
        lock.lockInterruptibly();
        PrioritizedSplitRunner result;
        try {
            while ((result = takeFirstSplit()) == null) {
                notEmpty.await();
            }
        }
        finally {
            lock.unlock();
        }

        return result;
    }

    private PrioritizedSplitRunner takeFirstSplit()
    {
        for (int i = 0; i < LEVELS.length; i++) {
            synchronized (levelWaitingSplits.get(i)) {
                while (true) {
                    PrioritizedSplitRunner split = levelWaitingSplits.get(i).poll();
                    if (split != null) {
                        // split was in the wrong level, send it to the right one
                        if (split.getLevel() != i) {
                            split.updateLevelPriority();
                            offer(split);
                            continue;
                        }
                        selectedLevelCounters.get(i).update(1);
                        return split;
                    }

                    break;
                }
            }
        }

        return null;
    }

    public void remove(PrioritizedSplitRunner split)
    {
        lock.lock();
        for (int i = 0; i < LEVELS.length; i++) {
            synchronized (levelWaitingSplits.get(i)) {
                levelWaitingSplits.get(i).remove(split);
            }
        }
        lock.unlock();
    }

    public void removeAll(Collection<PrioritizedSplitRunner> splits)
    {
        lock.lock();
        for (int i = 0; i < LEVELS.length; i++) {
            synchronized (levelWaitingSplits.get(i)) {
                levelWaitingSplits.get(i).removeAll(splits);
            }
        }
        lock.unlock();
    }

    public long getLevelMinPriority(int level, long taskThreadUsageNanos)
    {
        synchronized (levelMinPriority[level]) {
            levelMinPriority[level].compareAndSet(-1, taskThreadUsageNanos);
            PrioritizedSplitRunner split = levelWaitingSplits.get(level).peek();
            if (split != null) {
                long headLevelPriority = split.getLevelPriority();
                levelMinPriority[level].set(headLevelPriority);
                return headLevelPriority;
            }

            return levelMinPriority[level].get();
        }
    }

    public int size()
    {
        int waitingSplits = 0;
        for (int i = 0; i < LEVELS.length; i++) {
            waitingSplits += levelWaitingSplits.get(i).size();
        }

        return waitingSplits;
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
