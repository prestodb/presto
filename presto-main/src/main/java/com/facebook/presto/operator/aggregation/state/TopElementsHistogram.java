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
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

import java.io.Serializable;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class TopElementsHistogram<E> implements Serializable
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TopElementsHistogram.class).instanceSize();

    private int rowsProcessed=0;
    private int k;
    private ConservativeAddSketch ccms;
    private IndexedPriorityQueue<E> topEntries = new IndexedPriorityQueue<E>();


    /**
     * This class is to find heavy hitters and based upon the paper: http://theory.stanford.edu/~tim/s17/l/l2.pdf
     * @param k  User defined parameter to request only those values that occur atleast n/k times where n is the total count of values
     * @param epsError error bound such that counts are overestimated by at most epsError X n. Default value=1/2k
     * @param confidence probability that the count is overestimated by more than the error bound epsError X n. Default value=0.01
     * @param seed
     */
    public TopElementsHistogram(int k, double epsError, double confidence, int seed)
    {
        checkArgument(k >= 1, "maxEntries must be >= 1");
        requireNonNull(k, "maxEntries is null");

        this.k = k;
        this.ccms=new ConservativeAddSketch(epsError, confidence, seed);
    }

    public void add(E  item)
    {
        this.add(item, 1);
    }

    public void add(E  item, long count)
    {
        Long itemCount=ccms.add(item.toString(), count);
        rowsProcessed++;
        if(itemCount >= rowsProcessed/k) {
            boolean isAdded=topEntries.addOrUpdate(item, itemCount);
            //if a new value was added Trim the size of top entries maintained to conserve memory
            topEntries.removeBelowPriority((long)(rowsProcessed/k));
        }
    }

    public void merge(TopElementsHistogram... histograms)
    {
        if (histograms != null && histograms.length > 0) {
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
                Iterator<Entry<E>> elements = histogram.topEntries.iterator();
                while(elements.hasNext()){
                    Entry<E> item=elements.next();
                    //Estimate the count after the merger
                    Long itemCount=ccms.estimateCount(item.getValue().toString());
                    topEntries.addOrUpdate(item.getValue(), itemCount);
                }
                //Trim the size of top entries maintained to conserve memory
                topEntries.removeBelowPriority((long)(rowsProcessed/k));
            }
        }
    }


    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + ccms.estimatedInMemorySize() + GraphLayout.parseInstance(this.topEntries).totalSize();
    }

}
