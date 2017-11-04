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
package com.facebook.presto.operator;

import com.facebook.presto.spi.block.Block;
import io.airlift.stats.CounterStat;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class PagesIndexStats
{
    private final CounterStat totalBlocks = new CounterStat();
    private final CounterStat totalRetainedSize = new CounterStat();
    private final CounterStat totalCompactionsSubmitted = new CounterStat();
    private final CounterStat totalRetainedSizeSubmittedForCompaction = new CounterStat();

    private final CounterStat totalCompactionsSkipped = new CounterStat();
    private final CounterStat totalRetainedSizeSkippedCompaction = new CounterStat();
    private final CounterStat[] totalCompactionsInSavingBuckets = new CounterStat[10];
    private final CounterStat[] totalRetainedSizeInSavingBuckets = new CounterStat[10];

    public PagesIndexStats()
    {
        for (int i = 0; i < 10; i++) {
            totalCompactionsInSavingBuckets[i] = new CounterStat();
            totalRetainedSizeInSavingBuckets[i] = new CounterStat();
        }
    }

    public void recordBlock(Block block)
    {
        totalBlocks.update(1);
        totalRetainedSize.update(block.getRetainedSizeInBytes());
    }

    public void recordBlockCompaction(Block block, Block compactedBlock)
    {
        long retainedSize = block.getRetainedSizeInBytes();
        totalCompactionsSubmitted.update(1);
        totalRetainedSizeSubmittedForCompaction.update(retainedSize);

        if (block == compactedBlock) {
            totalCompactionsSkipped.update(1);
            totalRetainedSizeSkippedCompaction.update(retainedSize);
        }
        else {
            long compactedRetainedSize = compactedBlock.getRetainedSizeInBytes();
            int savingBucket = (int) ((retainedSize - compactedRetainedSize) / (double) (retainedSize) * 10.0);
            if (savingBucket >= 0 && savingBucket < 10) {
                totalCompactionsInSavingBuckets[savingBucket].update(1);
                totalRetainedSizeInSavingBuckets[savingBucket].update(retainedSize);
            }
        }
    }

    @Managed
    @Nested
    public CounterStat getTotalBlocks()
    {
        return totalBlocks;
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSize()
    {
        return totalRetainedSize;
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsSubmitted()
    {
        return totalCompactionsSubmitted;
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeSubmittedForCompaction()
    {
        return totalRetainedSizeSubmittedForCompaction;
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsSkipped()
    {
        return totalCompactionsSkipped;
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeSkippedCompaction()
    {
        return totalRetainedSizeSkippedCompaction;
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith0To10PctSaving()
    {
        return totalCompactionsInSavingBuckets[0];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith0To10PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[0];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith10To20PctSaving()
    {
        return totalCompactionsInSavingBuckets[1];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith10To20PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[1];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith20To30PctSaving()
    {
        return totalCompactionsInSavingBuckets[2];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith20To30PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[2];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith30To40PctSaving()
    {
        return totalCompactionsInSavingBuckets[3];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith30To40PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[3];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith40To50PctSaving()
    {
        return totalCompactionsInSavingBuckets[4];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith40To50PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[4];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith50To60PctSaving()
    {
        return totalCompactionsInSavingBuckets[5];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith50To60PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[5];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith60To70PctSaving()
    {
        return totalCompactionsInSavingBuckets[6];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith60To70PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[6];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith70To80PctSaving()
    {
        return totalCompactionsInSavingBuckets[7];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith70To80PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[7];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith80To90PctSaving()
    {
        return totalCompactionsInSavingBuckets[8];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith80To90PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[8];
    }

    @Managed
    @Nested
    public CounterStat getTotalCompactionsWith90To100PctSaving()
    {
        return totalCompactionsInSavingBuckets[9];
    }

    @Managed
    @Nested
    public CounterStat getTotalRetainedSizeWith90To100PctSaving()
    {
        return totalRetainedSizeInSavingBuckets[9];
    }
}
