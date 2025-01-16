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
package com.facebook.presto.operator.aggregation.approxmostfrequent.stream;

import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static io.airlift.slice.SizeOf.sizeOf;

public class IndexedPriorityQueue
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(IndexedPriorityQueue.class).instanceSize();
    public static final int MIN_POSITION = 0;
    private final StreamDataEntity[] minHeap;
    private final int heapCapacity;
    private final PriorityQueueDataChangeListener heapDataChangeListener;
    private final Comparator<StreamDataEntity> heapDataComparator;
    private int positionCount;

    public IndexedPriorityQueue(int heapCapacity, Comparator<StreamDataEntity> heapDataComparator, PriorityQueueDataChangeListener heapDataChangeListener)
    {
        this.minHeap = new StreamDataEntity[heapCapacity];
        this.heapCapacity = heapCapacity;
        this.heapDataChangeListener = heapDataChangeListener;
        this.heapDataComparator = heapDataComparator;
    }

    public boolean isFull()
    {
        return this.positionCount == heapCapacity;
    }

    public void add(StreamDataEntity data)
    {
        minHeap[positionCount++] = data;
        percolateUp(positionCount - 1);
    }

    public void replaceMin(StreamDataEntity data)
    {
        minHeap[MIN_POSITION] = data;
        percolateDown(MIN_POSITION);
    }

    public StreamDataEntity get(int index)
    {
        return minHeap[index];
    }

    public StreamDataEntity getMin()
    {
        return minHeap[MIN_POSITION];
    }

    public void percolateDown(int position)
    {
        int leftPosition;
        while ((leftPosition = position * 2 + 1) < positionCount) {
            int rightPosition = leftPosition + 1;
            int smallerChildPosition;
            if (rightPosition >= positionCount) {
                smallerChildPosition = leftPosition;
            }
            else {
                smallerChildPosition = heapDataComparator.compare(minHeap[leftPosition], minHeap[rightPosition]) >= 0 ? rightPosition : leftPosition;
            }
            if (heapDataComparator.compare(minHeap[smallerChildPosition], minHeap[position]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, smallerChildPosition);
            heapDataChangeListener.indexChanged(minHeap[position], position);
            position = smallerChildPosition;
        }
        heapDataChangeListener.indexChanged(minHeap[position], position);
    }

    private void swap(int position, int smallerChildPosition)
    {
        StreamDataEntity swapTemp = minHeap[position];
        minHeap[position] = minHeap[smallerChildPosition];
        minHeap[smallerChildPosition] = swapTemp;
    }

    public void percolateUp(int position)
    {
        while (position != 0) {
            int parentPosition = (position - 1) / 2;
            if (heapDataComparator.compare(minHeap[position], minHeap[parentPosition]) >= 0) {
                break; // child is larger or equal
            }
            swap(position, parentPosition);
            heapDataChangeListener.indexChanged(minHeap[position], position);
            position = parentPosition;
        }
        heapDataChangeListener.indexChanged(minHeap[position], position);
    }

    public int getSize()
    {
        return positionCount;
    }

    public List<StreamDataEntity> topK(int k, Comparator<StreamDataEntity> comparator)
    {
        ImmutableList.Builder<StreamDataEntity> builder = ImmutableList.builder();
        StreamDataEntity[] topKData = Arrays.copyOf(minHeap, positionCount);
        Arrays.sort(topKData, comparator);
        int totalCount = Math.min(k, positionCount);
        for (int position = 0; position < totalCount; position++) {
            builder.add(topKData[position]);
        }
        return builder.build();
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + sizeOf(minHeap);
    }
}
