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
package com.facebook.presto.operator.aggregation.mostfrequent.state;

import com.facebook.presto.operator.aggregation.mostfrequent.ConservativeAddSketch;
import com.facebook.presto.operator.aggregation.mostfrequent.IndexedPriorityQueue;
import com.facebook.presto.operator.aggregation.mostfrequent.IndexedPriorityQueue.Entry;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;

public class TopElementsHistogram
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TopElementsHistogram.class).instanceSize();

    private static final long MAX_FUNCTION_MEMORY = (long) Math.pow(2, 25);  // 32 MB https://www.aqua-calc.com/page/powers-of-two
    private static final int TOP_ENTRIES_SIZE = 1000;
    private static final int MAX_PERCENT = 100;
    private long rowsProcessed;
    private final double minPercentShare;
    private final int maxEntries;
    private ConservativeAddSketch sketch;
    private IndexedPriorityQueue topEntries = new IndexedPriorityQueue();
    private final double error;
    private final double confidence;
    private final int seed;

    /**
     * This class is to find heavy hitters and based upon the paper: http://theory.stanford.edu/~tim/s17/l/l2.pdf
     * It simply collects all elements into topEntries till maxEntries. Upon reaching the threshold converts to
     * probabilistic structure conservative-count-min-sketch
     * @param minPercentShare User defined parameter to request only those values that occur atleast n/k times where n is the total count of values
     * @param error error bound such that counts are overestimated by at most error X n. Default value=1/2k
     * @param confidence probability that the count is overestimated by more than the error bound error X n. Default value=0.99
     * @param seed
     */
    public TopElementsHistogram(double minPercentShare, double error, double confidence, int seed)
    {
        // TODO move the error calculation logic from ApproximateMostFrequentAggregations class to here
        checkArgument(minPercentShare > 0 && minPercentShare < MAX_PERCENT, "minPercentShare must be between 0 and 100");
        checkArgument(error > 0, "error bound must be greater than 0");
        checkArgument(confidence > 0 && confidence < 1, "confidence must be greater than 0 and less than 1");

        this.minPercentShare = minPercentShare;
        this.maxEntries = TOP_ENTRIES_SIZE;
        this.error = Math.min(error, minPercentShare / (2 * MAX_PERCENT));
        this.confidence = confidence;
        this.seed = seed;
    }

    public void add(Slice item)
    {
        this.add(item, 1);
    }

    public void add(Slice item, long count)
    {
        checkArgument((count >= 0), "Increment count cannot be negative");
        if (item == null) {
            // Mimic Presto's avg function behavior which ignores null both in the numerator and denominator
            return;
        }
        rowsProcessed += count;
        if (sketch == null && topEntries.size() < maxEntries) {
            topEntries.addOrIncrement(item, count);
            return;
        }
        if (sketch == null) {
            this.switchToSketch();
        }
        long itemCount = sketch.add(item, count);
        if (itemCount >= getMinItemCount() && itemCount > topEntries.getMinPriority()) {
            topEntries.addOrUpdate(item, itemCount);
            trimTopElements();
        }
    }

    public void add(List<Slice> list)
    {
        for (Slice item : list) {
            add(item);
        }
    }

    public void add(Map<Slice, Long> map)
    {
        for (Map.Entry<Slice, Long> entry : map.entrySet()) {
            add(entry.getKey(), entry.getValue());
        }
    }

    /**
     * All Count Min Sketches must be merged before merging even the first topEntries
     *
     * @param histograms
     */
    public void merge(TopElementsHistogram... histograms)
    {
        if (histograms == null || histograms.length <= 0) {
            //Nothing to merge
            return;
        }

        // try non-sketch merge first
        List<TopElementsHistogram> histogramList = new ArrayList<>(Arrays.asList(histograms));
        histogramList.add(this);

        boolean nullSketch = true;
        // since sketch merge may be required later collect result into a new container mergedHistogram
        TopElementsHistogram mergedHistogram = new TopElementsHistogram(minPercentShare, error, confidence, seed);
        Iterator<TopElementsHistogram> histogramIterator = histogramList.iterator();
        // Try non sketch based merge first
        while (histogramIterator.hasNext()) {
            TopElementsHistogram histogram = histogramIterator.next();
            if (histogram == null || histogram.rowsProcessed <= 0) {
                continue;
            }
            if (histogram.sketch != null || mergedHistogram.sketch != null) {
                nullSketch = false;
                break;
            }
            Iterator<Entry> iterator = histogram.topEntries.iterator();
            while (iterator.hasNext()) {
                Entry e = iterator.next();
                mergedHistogram.add(e.getValue(), e.getPriority());
            }
        }
        if (nullSketch && mergedHistogram.sketch == null) {
            // topEntries merged successfully and sketch is still null
            rowsProcessed = mergedHistogram.rowsProcessed;
            topEntries = mergedHistogram.topEntries;
            return;
        }

        // Code did not return yet that means non-sketch merge did not work!!
        // Merge ALL conservative-count-min-sketch and rowsProcessed before calling any estimateCount!!

        // Switch this instance of sketch if not already
        if (sketch == null) {
            this.switchToSketch();
        }

        for (TopElementsHistogram histogram : histograms) {
            if (histogram == null || histogram.rowsProcessed <= 0) {
                continue;
            }
            try {
                if (histogram.sketch == null) {
                    histogram.switchToSketch();
                }
                this.sketch.merge(histogram.sketch);
            }
            catch (Exception e) {
                throw new RuntimeException(e);
            }
            this.rowsProcessed += histogram.rowsProcessed;
        }

        // estimateCount can only be called after ALL conservative-count-min-sketch are merged.
        // Hence following loop cannot be merged with above loop (to improve efficiency)
        double minItemCount = getMinItemCount();
        histogramIterator = histogramList.iterator();
        while (histogramIterator.hasNext()) {
            TopElementsHistogram histogram = histogramIterator.next();
            Iterator<Slice> items = histogram.topEntries.keysIterator();
            while (items.hasNext()) {
                Slice item = items.next();
                //Estimate the count after the merger
                long itemCount = sketch.estimateCount((Slice) item);
                if (topEntries.size() < maxEntries || (itemCount >= minItemCount && itemCount > topEntries.getMinPriority())) {
                    topEntries.addOrUpdate(item, itemCount);
                    trimTopElements();
                }
            }
            histogramIterator.remove();  // free up memory
        }
    }

    public long getRowsProcessed()
    {
        return rowsProcessed;
    }

    public double getMinItemCount()
    {
        return minPercentShare * rowsProcessed / 100.0;
    }

    @VisibleForTesting
    public int getEntriesCount()
    {
        return topEntries.size();
    }

    @VisibleForTesting
    public int getMaxEntries()
    {
        return maxEntries;
    }

    public long estimateCount(Slice item)
    {
        return sketch.estimateCount(item);
    }

    /**
     * Everytime the top entries has to trimmed, we need to re-calculate bottom most elements as they may have had conflict
     * and hence their estimate count may have changed. If we use the old estimate count (which is accurate) we may drop it
     * from the top list. Problem is that this makes the function non-deterministic: If the element comes earlier and gets dropped
     * it does not make it to final list. However if it comes in after the other elements with which it has conflict
     * then it does not get dropped since the new estimate count is much higher due to conflict. To make the function
     * more deterministic (cannot be 100% deterministic), estimate count needs to be recalculated for elements before dropping
     */
    public void trimTopElements()
    {
        if (topEntries.size() <= maxEntries || sketch == null) {
            // If the size is less than maxEntries don't bother trimming
            return;
        }
        double minItemCount = getMinItemCount();
        int loopCount = topEntries.size();
        for (int i = 0; i < loopCount; i++) {
            Entry entry = topEntries.peek();
            Slice item = entry.getValue();
            long itemCount = sketch.estimateCount(item);
            if (itemCount >= minItemCount && itemCount > entry.getPriority()) {
                // itemCount meets minimum threshold && itemCount has increased which means may not be the least anymore
                topEntries.addOrUpdate(item, itemCount);
            }
            else {
                topEntries.remove(item);
                if (topEntries.size() <= maxEntries) {
                    return;
                }
            }
        }
        // Remove all items which are beyond maxEntries.
        topEntries.poll(topEntries.size() - maxEntries);
    }

    public Map<Slice, Long> getTopElements()
    {
        double minItemCount = getMinItemCount();
        Iterator<Entry> elements = this.topEntries.iterator();
        Map<Slice, Long> topElements = new HashMap<>();
        while (elements.hasNext()) {
            Entry e = elements.next();
            Slice item = e.getValue();
            long itemCount = e.getPriority();
            if (sketch != null) {
                itemCount = sketch.estimateCount(item);
            }
            if (itemCount >= minItemCount) {
                topElements.put(item, itemCount);
            }
        }
        return topElements;
    }

    public long estimatedInMemorySize()
    {
        long estimatedMemorySize = INSTANCE_SIZE + this.topEntries.estimatedInMemorySize();
        if (sketch != null) {
            estimatedMemorySize += sketch.estimatedInMemorySize();
        }
        if (estimatedMemorySize > MAX_FUNCTION_MEMORY) {
            throw new RuntimeException("Memory used: " + estimatedMemorySize + ", breached max allowed memory of " + MAX_FUNCTION_MEMORY + ".\n Reducing error-bound/confidence OR increasing min_percent_share may help.");
        }
        return estimatedMemorySize;
    }

    void switchToSketch()
    {
        if (sketch != null) {
            return;
        }
        // Conservative count min sketch is only initialized when the number of distinct elements crosses maxEntries threshold
        this.sketch = new ConservativeAddSketch(error, confidence, seed);
        Iterator<Entry> iterator = topEntries.iterator();
        while (iterator.hasNext()) {
            Entry e = iterator.next();
            sketch.add(e.getValue(), e.getPriority());
        }
    }

    boolean isPrecise()
    {
        // If sketch is null, probabilistic structure is not used yet and the counts are precise
        return (sketch == null);
    }

    public Slice serialize()
    {
        SliceOutput s = new DynamicSliceOutput((int) estimatedInMemorySize());
        s.writeLong(rowsProcessed);
        s.writeDouble(minPercentShare);
        s.writeInt(maxEntries);
        s.writeDouble(error);
        s.writeDouble(confidence);
        s.writeInt(seed);

        if (sketch != null) {
            Slice sketchSlice = sketch.serialize();
            s.writeInt(sketchSlice.length());
            s.writeBytes(sketchSlice);
        }
        else {
            s.writeInt(0);
        }

        Slice slcTopEntries = topEntries.serialize();
        s.writeInt(slcTopEntries.length());
        s.writeBytes(slcTopEntries);
        return s.slice();
    }

    TopElementsHistogram(Slice serialized)
    {
        SliceInput s = new BasicSliceInput(serialized);
        rowsProcessed = s.readLong();
        minPercentShare = s.readDouble();
        maxEntries = s.readInt();
        error = s.readDouble();
        confidence = s.readDouble();
        seed = s.readInt();
        int sketchSize = s.readInt();
        if (sketchSize > 0) {
            sketch = new ConservativeAddSketch(s.readSlice(sketchSize));
        }

        int topEntriesSize = s.readInt();
        topEntries = new IndexedPriorityQueue(s.readSlice(topEntriesSize));
    }

    @Override
    public boolean equals(Object o)
    {
        if (o == this) {
            return true;
        }
        if (!(o instanceof TopElementsHistogram)) {
            return false;
        }

        TopElementsHistogram c = (TopElementsHistogram) o;

        return rowsProcessed == c.rowsProcessed
                && Double.compare(minPercentShare, c.minPercentShare) == 0
                && maxEntries == c.maxEntries
                && Double.compare(error, c.error) == 0
                && Double.compare(confidence, c.confidence) == 0
                && seed == c.seed
                && ((sketch == null && c.sketch == null) || sketch.equals(c.sketch))
                && getTopElements().equals(c.getTopElements());
    }

    @Override
    public int hashCode()
    {
        int result;
        result = Long.hashCode(rowsProcessed);
        result = 31 * result + Double.hashCode(minPercentShare);
        result = 31 * result + maxEntries;
        result = 31 * result + Double.hashCode(error);
        result = 31 * result + Double.hashCode(confidence);
        result = 31 * result + seed;
        if (sketch != null) {
            result = 31 * result + sketch.hashCode();
        }
        result = 31 * result + topEntries.hashCode();
        return result;
    }
}
