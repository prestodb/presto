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
package com.facebook.presto.operator.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Doubles;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import org.openjdk.jol.info.ClassLayout;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class NumericHistogram
{
    private static final byte FORMAT_TAG = 0;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NumericHistogram.class).instanceSize();

    private static final Comparator<Entry> COMPARATOR = new Comparator<Entry>()
    {
        @Override
        public int compare(Entry first, Entry second)
        {
            int result = Double.compare(first.getPenalty(), second.getPenalty());
            if (result == 0) {
                result = Integer.compare(first.id, second.id);
            }
            return result;
        }
    };

    private final int maxBuckets;
    private final double[] values;
    private final double[] weights;

    private int nextIndex = 0;

    public NumericHistogram(int maxBuckets)
    {
        this(maxBuckets, Math.max((int) (maxBuckets * 0.2), 1));
    }

    public NumericHistogram(int maxBuckets, int buffer)
    {
        checkArgument(maxBuckets >= 2, "maxBuckets must be >= 2");
        checkArgument(buffer >= 1, "buffer must be >= 1");

        this.maxBuckets = maxBuckets;
        this.values = new double[maxBuckets + buffer];
        this.weights = new double[maxBuckets + buffer];
    }

    public NumericHistogram(Slice serialized, int buffer)
    {
        checkNotNull(serialized, "serialized is null");
        checkArgument(buffer >= 1, "buffer must be >= 1");

        SliceInput input = serialized.getInput();

        Preconditions.checkArgument(input.readByte() == FORMAT_TAG, "Unsupported format tag");

        maxBuckets = input.readInt();
        nextIndex = input.readInt();
        values = new double[maxBuckets + buffer];
        weights = new double[maxBuckets + buffer];

        input.readBytes(Slices.wrappedDoubleArray(values), nextIndex * SizeOf.SIZE_OF_DOUBLE);
        input.readBytes(Slices.wrappedDoubleArray(weights), nextIndex * SizeOf.SIZE_OF_DOUBLE);
    }

    public Slice serialize()
    {
        compact();

        int requiredBytes = SizeOf.SIZE_OF_BYTE + // format
                SizeOf.SIZE_OF_INT + // max buckets
                SizeOf.SIZE_OF_INT + // entry count
                SizeOf.SIZE_OF_DOUBLE * nextIndex + // values
                SizeOf.SIZE_OF_DOUBLE * nextIndex; // weights

        return Slices.allocate(requiredBytes)
                .getOutput()
                .appendByte(FORMAT_TAG)
                .appendInt(maxBuckets)
                .appendInt(nextIndex)
                .appendBytes(Slices.wrappedDoubleArray(values, 0, nextIndex))
                .appendBytes(Slices.wrappedDoubleArray(weights, 0, nextIndex))
                .slice();
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + SizeOf.sizeOf(values) + SizeOf.sizeOf(weights);
    }

    public void add(double value)
    {
        add(value, 1);
    }

    public void add(double value, double weight)
    {
        if (nextIndex == values.length) {
            trim(maxBuckets);
        }

        values[nextIndex] = value;
        weights[nextIndex] = weight;

        nextIndex++;
    }

    public void mergeWith(NumericHistogram other)
    {
        int count = nextIndex + other.nextIndex;

        double[] newValues = new double[count];
        double[] newWeights = new double[count];

        concat(newValues, this.values, this.nextIndex, other.values, other.nextIndex);
        concat(newWeights, this.weights, this.nextIndex, other.weights, other.nextIndex);

        sort(newValues, newWeights, count);

        count = mergeSameBuckets(newValues, newWeights, count);

        if (count <= maxBuckets) {
            // copy back into this.values/this.weights
            System.arraycopy(newValues, 0, this.values, 0, count);
            System.arraycopy(newWeights, 0, this.weights, 0, count);
            nextIndex = count;
            return;
        }

        PriorityQueue<Entry> queue = initializeQueue(newValues, newWeights, count);
        mergeEntries(queue, maxBuckets);
        store(queue);
    }

    public Map<Double, Double> getBuckets()
    {
        compact();

        Map<Double, Double> result = new LinkedHashMap<>();
        for (int i = 0; i < nextIndex; i++) {
            result.put(values[i], weights[i]);
        }
        return result;
    }

    @VisibleForTesting
    public void compact()
    {
        trim(maxBuckets);
    }

    private void trim(int target)
    {
        sort(values, weights, nextIndex);
        nextIndex = mergeSameBuckets(values, weights, nextIndex);

        if (nextIndex <= target) {
            return;
        }

        PriorityQueue<Entry> queue = initializeQueue(values, weights, nextIndex);
        mergeEntries(queue, target);
        store(queue);

        sort(values, weights, nextIndex);
    }

    private static void mergeEntries(PriorityQueue<Entry> queue, int targetCount)
    {
        int count = queue.size();

        while (count > targetCount) {
            Entry current = queue.poll();
            if (!current.isValid()) {
                continue;
            }

            count--;

            Entry right = current.getRight();

            // right is guaranteed to exist because we set the penalty of the last bucket to infinity
            // so the first current in the queue can never be the last bucket
            checkState(right != null, "Expected right to be != null");
            checkState(right.isValid(), "Expected right to be valid");

            // merge "current" with "right"
            double newWeight = current.getWeight() + right.getWeight();
            double newValue = (current.getValue() * current.getWeight() + right.getValue() * right.getWeight()) / newWeight;

            // mark "right" as invalid so we can skip it if it shows up as we poll from the head of the queue
            right.invalidate();

            // compute the merged entry
            Entry merged;
            Entry newRight = right.getRight();
            if (newRight == null) {
                merged = new Entry(current.id, newValue, newWeight, Double.POSITIVE_INFINITY);
            }
            else {
                merged = new Entry(current.id, newValue, newWeight, computePenalty(newValue, newWeight, newRight.getValue(), newRight.getWeight()));
                link(merged, newRight);
            }
            queue.add(merged);

            Entry left = current.getLeft();
            if (left != null) {
                checkState(left.isValid(), "Expected left to be valid");
                // replace "left" with a new entry with a penalty adjusted to account for (newValue, newWeight)
                left.invalidate();

                Entry newLeft = new Entry(left.id, left.getValue(), left.getWeight(), computePenalty(left.getValue(), left.getWeight(), newValue, newWeight));
                link(newLeft, merged);
                queue.add(newLeft);

                if (left.getLeft() != null) {
                    link(left.getLeft(), newLeft);
                }
            }
        }
    }

    /**
     * Dump the entries in the queue back into the bucket arrays
     */
    private void store(PriorityQueue<Entry> queue)
    {
        nextIndex = 0;
        for (Entry entry : queue) {
            if (entry.isValid()) {
                values[nextIndex] = entry.getValue();
                weights[nextIndex] = entry.getWeight();
                nextIndex++;
            }
        }
    }

    /**
     * Copy two arrays back-to-back onto the target array starting at offset 0
     */
    private static void concat(double[] target, double[] first, int firstLength, double[] second, int secondLength)
    {
        System.arraycopy(first, 0, target, 0, firstLength);
        System.arraycopy(second, 0, target, firstLength, secondLength);
    }

    /**
     * Simple pass that merges buckets with the same "x" value
     * The buckets must be sorted before this method is called
     */
    private static int mergeSameBuckets(double[] values, double[] weights, int nextIndex)
    {
        int current = 0;
        for (int i = 1; i < nextIndex; i++) {
            if (values[current] == values[i]) {
                weights[current] += weights[i];
            }
            else {
                current++;
                values[current] = values[i];
                weights[current] = weights[i];
            }
        }
        return current + 1;
    }

    /**
     * Create a priority queue with an entry for each bucket, ordered by the penalty score with respect to the bucket to its right
     * The last bucket has a penalty of infinity
     * Entries are doubly-linked to keep track of the relative position of each bucket
     */
    private static PriorityQueue<Entry> initializeQueue(double[] values, double[] weights, int nextIndex)
    {
        PriorityQueue<Entry> queue = new PriorityQueue<>(nextIndex, COMPARATOR);

        Entry previous = new Entry(0, values[0], weights[0], computePenalty(values[0], weights[0], values[1], weights[1]));
        queue.add(previous);
        for (int i = 1; i < nextIndex; i++) {
            double penalty = Double.POSITIVE_INFINITY;
            if (i < nextIndex - 1) {
                penalty = computePenalty(values[i], weights[i], values[i + 1], weights[i + 1]);
            }

            Entry current = new Entry(i, values[i], weights[i], penalty);
            link(previous, current);
            queue.add(current);

            previous = current;
        }

        return queue;
    }

    private static void link(Entry left, Entry right)
    {
        right.setLeft(left);
        left.setRight(right);
    }

    private static void sort(final double[] values, final double[] weights, int nextIndex)
    {
        // sort x and y value arrays based on the x values
        Arrays.quickSort(0, nextIndex, new AbstractIntComparator()
        {
            @Override
            public int compare(int a, int b)
            {
                return Doubles.compare(values[a], values[b]);
            }
        }, new Swapper()
        {
            @Override
            public void swap(int a, int b)
            {
                double temp = values[a];
                values[a] = values[b];
                values[b] = temp;

                temp = weights[a];
                weights[a] = weights[b];
                weights[b] = temp;
            }
        });
    }

    private static double computePenalty(double x1, double y1, double x2, double y2)
    {
        double weight = y1 + y2;
        double squaredDifference = (x1 - x2) * (x1 - x2);
        double proportionsProduct = (y1 * y2) / ((y1 + y2) * (y1 + y2));
        return weight * squaredDifference * proportionsProduct;
    }

    private static class Entry
    {
        private final double penalty;

        private final int id;
        private final double value;
        private final double weight;

        private boolean valid = true;
        private Entry left;
        private Entry right;

        private Entry(int id, double value, double weight, double penalty)
        {
            this.id = id;
            this.value = value;
            this.weight = weight;
            this.penalty = penalty;
        }

        public Entry getLeft()
        {
            return left;
        }

        public void setLeft(Entry left)
        {
            this.left = left;
        }

        public Entry getRight()
        {
            return right;
        }

        public void setRight(Entry right)
        {
            this.right = right;
        }

        public double getValue()
        {
            return value;
        }

        public double getWeight()
        {
            return weight;
        }

        public double getPenalty()
        {
            return penalty;
        }

        public boolean isValid()
        {
            return valid;
        }

        public void invalidate()
        {
            this.valid = false;
        }
    }
}
