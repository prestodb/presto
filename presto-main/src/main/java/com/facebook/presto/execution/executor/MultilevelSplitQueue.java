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

import com.google.common.annotations.VisibleForTesting;
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
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class MultilevelSplitQueue
{
    static final long LEVEL_CONTRIBUTION_CAP = SECONDS.toNanos(30);
    static final int[] LEVEL_THRESHOLD_SECONDS = {0, 1, 10, 60, 300};

    @GuardedBy("lock")
    private final List<PriorityQueue<PrioritizedSplitRunner>> levelWaitingSplits;
    @GuardedBy("lock")
    private final long[] levelScheduledTime = new long[LEVEL_THRESHOLD_SECONDS.length];

    private final AtomicLong[] levelMinPriority;
    private final List<CounterStat> selectedLevelCounters;

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();

    private final boolean levelAbsolutePriority;
    private final double levelPriorityMultiplier;

    public MultilevelSplitQueue(boolean levelAbsolutePriority, double levelPriorityMultiplier)
    {
        this.levelMinPriority = new AtomicLong[LEVEL_THRESHOLD_SECONDS.length];
        this.levelWaitingSplits = new ArrayList<>(LEVEL_THRESHOLD_SECONDS.length);
        ImmutableList.Builder<CounterStat> counters = ImmutableList.builder();

        for (int i = 0; i < LEVEL_THRESHOLD_SECONDS.length; i++) {
            levelMinPriority[i] = new AtomicLong(-1);
            levelWaitingSplits.add(new PriorityQueue<>());
            counters.add(new CounterStat());
        }

        this.selectedLevelCounters = counters.build();

        this.levelAbsolutePriority = levelAbsolutePriority;
        this.levelPriorityMultiplier = levelPriorityMultiplier;
    }

    public void addLevelTime(int level, long nanos)
    {
        lock.lock();
        try {
            if (levelScheduledTime[level] == -1) {
                levelScheduledTime[level] = 0;
            }
            levelScheduledTime[level] += nanos;
        }
        finally {
            lock.unlock();
        }
    }

    public void offer(PrioritizedSplitRunner split)
    {
        checkArgument(split != null, "split is null");

        split.setReady();
        lock.lock();
        try {
            levelWaitingSplits.get(split.getPriority().getLevel()).offer(split);
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
                while ((result = pollSplit()) == null) {
                    notEmpty.await();
                }

                if (result.updateLevelPriority()) {
                    offer(result);
                    continue;
                }

                int selectedLevel = result.getPriority().getLevel();
                levelMinPriority[selectedLevel].set(result.getPriority().getLevelPriority());
                selectedLevelCounters.get(selectedLevel).update(1);

                return result;
            }
            finally {
                lock.unlock();
            }
        }
    }

    @GuardedBy("lock")
    private PrioritizedSplitRunner pollSplit()
    {
        if (levelAbsolutePriority) {
            return pollFirstSplit();
        }

        long expectedScheduledTime = updateLevelTimes();
        double worstRatio = 1;
        int selectedLevel = -1;
        for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
            if (!levelWaitingSplits.get(level).isEmpty()) {
                double ratio = levelScheduledTime[level] == 0 ? 0 : expectedScheduledTime / (1.0 * levelScheduledTime[level]);
                if (selectedLevel == -1 || ratio > worstRatio) {
                    worstRatio = ratio;
                    selectedLevel = level;
                }
            }

            expectedScheduledTime /= levelPriorityMultiplier;
        }

        if (selectedLevel == -1) {
            return null;
        }

        return levelWaitingSplits.get(selectedLevel).poll();
    }

    @GuardedBy("lock")
    private long updateLevelTimes()
    {
        long level0ExpectedTime = levelScheduledTime[0];
        boolean updated;
        do {
            double currentMultiplier = levelPriorityMultiplier;
            updated = false;
            for (int level = 0; level < LEVEL_THRESHOLD_SECONDS.length; level++) {
                currentMultiplier /= levelPriorityMultiplier;
                long levelExpectedTime = (long) (level0ExpectedTime * currentMultiplier);

                if (levelWaitingSplits.get(level).isEmpty()) {
                    levelScheduledTime[level] = levelExpectedTime;
                    continue;
                }

                if (levelScheduledTime[level] > levelExpectedTime) {
                    level0ExpectedTime = levelScheduledTime[level] * (long) Math.pow(levelPriorityMultiplier, level);
                    updated = true;
                    break;
                }
            }
        } while (updated && level0ExpectedTime != 0);

        return level0ExpectedTime;
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

    public Priority updatePriority(Priority oldPriority, long quantaNanos, long scheduledNanos)
    {
        int oldLevel = oldPriority.getLevel();
        int newLevel = computeLevel(scheduledNanos);
        long levelContribution = Math.min(quantaNanos, LEVEL_CONTRIBUTION_CAP);

        if (oldLevel == newLevel) {
            addLevelTime(oldLevel, levelContribution);
            return new Priority(oldLevel, oldPriority.getLevelPriority() + quantaNanos);
        }

        long remainingLevelContribution = levelContribution;
        long remainingTaskTime = quantaNanos;

        for (int currentLevel = oldLevel; currentLevel < newLevel; currentLevel++) {
            long timeAccruedToLevel = Math.min(SECONDS.toNanos(LEVEL_THRESHOLD_SECONDS[currentLevel + 1] - LEVEL_THRESHOLD_SECONDS[currentLevel]), remainingLevelContribution);
            addLevelTime(currentLevel, timeAccruedToLevel);
            remainingLevelContribution -= timeAccruedToLevel;
            remainingTaskTime -= timeAccruedToLevel;
        }

        addLevelTime(newLevel, remainingLevelContribution);
        long newLevelMinPriority = getLevelMinPriority(newLevel, scheduledNanos);
        return new Priority(newLevel, newLevelMinPriority + remainingTaskTime);
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
        for (int i = 0; i < (LEVEL_THRESHOLD_SECONDS.length - 1); i++) {
            if (seconds < LEVEL_THRESHOLD_SECONDS[i + 1]) {
                return i;
            }
        }

        return LEVEL_THRESHOLD_SECONDS.length - 1;
    }

    @VisibleForTesting
    long[] getLevelScheduledTime()
    {
        lock.lock();
        try {
            return levelScheduledTime;
        }
        finally {
            lock.unlock();
        }
    }
}
