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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.operator.aggregation.heavyhitters.CountMinSketch;
import com.facebook.presto.operator.aggregation.heavyhitters.IndexedPriorityQueue;
import com.facebook.presto.operator.aggregation.heavyhitters.IndexedPriorityQueue.Entry;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.*;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TopElementsHistogram<E>
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TopElementsHistogram.class).instanceSize();

    private int rowsProcessed=0;
    private double minPercentShare =100;
    private int maxEntries;
    private CountMinSketch ccms;
    private IndexedPriorityQueue<E> topEntries = new IndexedPriorityQueue<E>();


    /**
     * This class is to find heavy hitters and based upon the paper: http://theory.stanford.edu/~tim/s17/l/l2.pdf
     * @param min_percent_share  User defined parameter to request only those values that occur atleast n/k times where n is the total count of values
     * @param epsError error bound such that counts are overestimated by at most epsError X n. Default value=1/2k
     * @param confidence probability that the count is overestimated by more than the error bound epsError X n. Default value=0.01
     * @param seed
     */
    public TopElementsHistogram(double min_percent_share, double epsError, double confidence, int seed)
    {
        checkArgument((0 <= min_percent_share && min_percent_share <= 100), "minPercentShare must be between 0 and 100");
        requireNonNull(min_percent_share, "minPercentShare is null");

        this.minPercentShare = min_percent_share;
        this.maxEntries = (int)Math.ceil(100/this.minPercentShare); //there can be more than maxEntries because of over counting of CountMinSketch
        this.ccms=new CountMinSketch(epsError, confidence, seed);
    }

    public void add(E  item)
    {
        this.add(item, 1);
    }

    public void add(E  item, long count)
    {
        if(item == null) {
            // Mimic Presto's avg function behavior which ignores null both in the numerator and denominator
            return;
        }
        Long itemCount=ccms.add(item.toString(), count);
        rowsProcessed += count;
        // This is the only place where trim can be done before adding since only one element is being added
        // And we have handle to that one element.
        if(100.0*itemCount/rowsProcessed >= minPercentShare) {
            topEntries.addOrUpdate(item, itemCount);
        }
        trimTopElements();
    }

    public void add(List<E> list){
        for(E item: list){
            add(item);
        }
    }

    public void add(Map<E, Long> map){
        for(Map.Entry<E, Long> entry: map.entrySet()){
            add(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Be EXTREMELY careful in changing this method.
     * Changes here can result in many unintended consequences in the edge cases
     * @param histograms
     */
    public void merge(TopElementsHistogram<E>... histograms)
    {
        if (histograms == null || histograms.length <= 0) {
            //Nothing to merge
            return;
        }

        // Merge ALL conservative-count-min-sketch and rowsProcessed before calling any estimateCount!!
        for (TopElementsHistogram<E> histogram : histograms) {
            if (histogram == null || histogram.rowsProcessed <= 0)
                continue;
            try {
                this.ccms.merge(histogram.ccms);
            }catch(Exception e){
                //TODO how to handle the CountMinSketch.CMSMergeException
                // Shouldn't happen
                throw new RuntimeException(e);
            }
            this.rowsProcessed += histogram.rowsProcessed;
        }

        // DON'T merge this "for" loop with the previous one. It will impact the behaviour of the class
        // All elements must be counted after merging ALL conservative-count-min-sketch for accuracy
        for (TopElementsHistogram<E> histogram : histograms) {
            Iterator<E> elements = this.topEntries.keysIterator();
            while(elements.hasNext()){
                E item=elements.next();
                //Estimate the count after the merger
                Long itemCount=ccms.estimateCount(item.toString());
                topEntries.addOrUpdate(item, itemCount);
            }
        }
        // Elements from "this-instance" must be counted again after the conservative-count-min-sketch merger
        Iterator<E> elements = this.topEntries.keysIterator();
        //TODO is it ok to update values of the iterator's underlying map?
        while(elements.hasNext()){
            E item=elements.next();
            //Estimate the count after the merger
            Long itemCount=ccms.estimateCount(item.toString());
            topEntries.addOrUpdate(item, itemCount);
        }

        // DON'T trim earlier for conserving memory. Trimming can only be done at the end.
        // If done earlier we may loose items due to increased rowsProcessed
        trimTopElements();
    }

    public long getRowsProcessed(){
        return rowsProcessed;
    }

    public long estimateCount(E item){
        return ccms.estimateCount(item.toString());
    }

    public long estimateMeanCount(E item){
        return ccms.estimateMeanCount(item.toString());
    }

    /**
     *  Everytime the top entries has to trimmed, we need to re-calculate bottom most elements as they may have had conflict
     *  and hence their estimate count may have changed. If we use the old estimate count (which is accurate) we may drop it
     *  from the top list. Problem is that this makes the function non-deterministic. If the element comes early on and gets dropped
     *  it does not make it to final list. However if it comes in after the other elements with which it has conflict
     *  then it does not get dropped since the new estimate count is much higher due to conflict. To make the function deterministic,
     *  estimate count needs to be recalculated for all elements before dropping
     */
    public void trimTopElements(){
        if(topEntries.size() <= maxEntries){
            // If the size is less than maxEntries don't bother trimming
            return;
        }
        double minItemCount = minPercentShare * rowsProcessed / 100.0;
        int loopCount = topEntries.size();
        for(int i=0; i<loopCount; i++) {
            Entry entry = topEntries.peek();
            if(entry.getPriority() >= minItemCount){
                //If the previous itemCount is already above the minItemCount, no further trimming possible
                return;
            }
            E item = (E)entry.getValue();
            long itemCount = ccms.estimateCount(item.toString());
            if(itemCount >=minItemCount){
                topEntries.addOrUpdate(item, itemCount);
            }else{
                topEntries.remove(item);
            }
        }
    }

    public Map<E, Long> getTopElements(){
        double minItemCount = minPercentShare * rowsProcessed / 100.0;
        Iterator<Entry<E>> elements = this.topEntries.iterator();
        Map<E, Long> topElements = new HashMap<E, Long>();
        while(elements.hasNext()){
            Entry<E> e = elements.next();
            E item = e.getValue();
            long itemCount = ccms.estimateCount(item.toString());
            if(itemCount >= minItemCount){
                topElements.put(item, itemCount);
            }
        }
        return topElements;
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + ccms.estimatedInMemorySize() + this.topEntries.estimatedInMemorySize();
    }

    public Slice serialize(){
        SliceOutput s = new DynamicSliceOutput((int)estimatedInMemorySize());
        s.writeInt(rowsProcessed);
        s.writeDouble(minPercentShare);

        Slice slcCcms = ccms.serialize();
        s.writeInt(slcCcms.length());
        s.writeBytes(slcCcms);

        Slice slcTopEntries = topEntries.serialize();
        s.writeInt(slcTopEntries.length());
        s.writeBytes(slcTopEntries);
        return s.slice();
    }

    @VisibleForTesting
    public TopElementsHistogram(Slice serialized){
        SliceInput s = new BasicSliceInput(serialized);
        rowsProcessed = s.readInt();
        minPercentShare = s.readDouble();
        int ccmsSize = s.readInt();
        ccms = new CountMinSketch(s.readSlice(ccmsSize));

        int topEntriesSize = s.readInt();
        topEntries = new IndexedPriorityQueue(s.readSlice(topEntriesSize));
    }

}
