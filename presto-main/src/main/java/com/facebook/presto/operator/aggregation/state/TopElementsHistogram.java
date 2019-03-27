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

import com.facebook.presto.operator.aggregation.heavyhitters.ConservativeAddSketch;
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
    private double min_percent_share=100;
    private ConservativeAddSketch ccms;
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
        checkArgument((0 <= min_percent_share && min_percent_share <= 100), "min_percent_share must be between 0 and 100");
        requireNonNull(min_percent_share, "min_percent_share is null");

        this.min_percent_share = min_percent_share;   //k = (min_percent_share*n)/100
        this.ccms=new ConservativeAddSketch(epsError, confidence, seed);
    }

    public Map<E, Long> getTopElements(){
        Iterator<Entry<E>> elements = this.topEntries.iterator();
        Map<E, Long> topElements = new HashMap<E, Long>();
        while(elements.hasNext()){
            Entry<E> e = elements.next();
            topElements.put(e.getValue(), e.getPriority());
        }
        return topElements;
    }

    public void add(E  item)
    {
        this.add(item, 1);
    }

    public void add(E  item, long count)
    {
        if(item == null) {
            // Mimic Presto's avg function behavious which ignores null both in the numerator and denominator
            return;
        }
        Long itemCount=ccms.add(item.toString(), count);
        rowsProcessed += count;
        // This is the only place where trim can be done before adding since only one element is being added
        // And we have handle to that one element.
        trimTopElements();
        if(100.0*itemCount/rowsProcessed >= min_percent_share) {
            topEntries.addOrUpdate(item, itemCount);
        }
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

    public void trimTopElements(){
        double minItemCount = min_percent_share * rowsProcessed / 100.0;
        if(topEntries.getMinPriority() < minItemCount) {
            topEntries.removeBelowPriority(minItemCount);
        }
    }

    /**
     * Be EXTREMELY careful in changing this method.
     * Changes here can result in many unintended consequences in the edge cases
     * @param histograms
     */
    public void merge(TopElementsHistogram... histograms)
    {
        if (histograms != null && histograms.length > 0) {
            // Merge ALL conservative-count-min-sketch and rowsProcessed before calling any estimateCount!!
            for (TopElementsHistogram histogram : histograms) {
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
            for (TopElementsHistogram histogram : histograms) {
                Iterator<E> elements = this.topEntries.keysIterator();
                while(elements.hasNext()){
                    E item=elements.next();
                    //Estimate the count after the merger
                    Long itemCount=ccms.estimateCount(item.toString());
                    topEntries.addOrUpdate(item, itemCount);
                }
            }
        }
        // Elements from "this-instance" must be counted again after the conservative-count-min-sketch merger
        Iterator<E> elements = this.topEntries.keysIterator();
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

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + ccms.estimatedInMemorySize() + this.topEntries.estimatedInMemorySize();
    }

    public Slice serialize(){
        SliceOutput s = new DynamicSliceOutput((int)estimatedInMemorySize());
        s.writeInt(rowsProcessed);
        s.writeDouble(min_percent_share);

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
        min_percent_share = s.readDouble();
        int ccmsSize = s.readInt();
        ccms = new ConservativeAddSketch(s.readSlice(ccmsSize));

        int topEntriesSize = s.readInt();
        topEntries = new IndexedPriorityQueue(s.readSlice(topEntriesSize));
    }

}
