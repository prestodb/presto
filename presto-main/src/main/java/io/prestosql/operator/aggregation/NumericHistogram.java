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
package io.prestosql.operator.aggregation;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.Slices;
import it.unimi.dsi.fastutil.Arrays;
import it.unimi.dsi.fastutil.Swapper;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import org.openjdk.jol.info.ClassLayout;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class NumericHistogram
{
    private static final byte FORMAT_TAG = 0;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(NumericHistogram.class).instanceSize();

    private final int maxBuckets;
    private final double[] values;
    private final double[] weights;

    private int nextIndex;

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
        requireNonNull(serialized, "serialized is null");
        checkArgument(buffer >= 1, "buffer must be >= 1");

        SliceInput input = serialized.getInput();

        checkArgument(input.readByte() == FORMAT_TAG, "Unsupported format tag");

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
                .getUnderlyingSlice();
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
            compact();
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

        count = mergeSameBuckets(newValues, newWeights, count);

        if (count <= maxBuckets) {
            // copy back into this.values/this.weights
            System.arraycopy(newValues, 0, this.values, 0, count);
            System.arraycopy(newWeights, 0, this.weights, 0, count);
            nextIndex = count;
            return;
        }

        sort(newValues, newWeights, count);
        store(mergeBuckets(newValues, newWeights, count, maxBuckets));
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
    void compact()
    {
        nextIndex = mergeSameBuckets(values, weights, nextIndex);

        if (nextIndex <= maxBuckets) {
            return;
        }

        // entries are guaranteed to be sorted as a side-effect of the call to mergeSameBuckets
        store(mergeBuckets(values, weights, nextIndex, maxBuckets));
    }

    private static PriorityQueue<Entry> mergeBuckets(double[] values, double[] weights, int count, int targetCount)
    {
        checkArgument(targetCount > 0, "targetCount must be > 0");

        PriorityQueue<Entry> queue = initializeQueue(values, weights, count);

        while (count > targetCount) {
            Entry current = queue.poll();
            if (!current.isValid()) {
                // ignore entries that have already been replaced
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

            // compute the merged entry linked to right of right
            Entry merged = new Entry(current.getId(), newValue, newWeight, right.getRight());
            queue.add(merged);

            Entry left = current.getLeft();
            if (left != null) {
                checkState(left.isValid(), "Expected left to be valid");

                // replace "left" with a new entry with a penalty adjusted to account for (newValue, newWeight)
                left.invalidate();

                // create a new left entry linked to the merged entry
                queue.add(new Entry(left.getId(), left.getValue(), left.getWeight(), left.getLeft(), merged));
            }
        }

        return queue;
    }

    /**
     * Dump the entries in the queue back into the bucket arrays
     * The values are guaranteed to be sorted in increasing order after this method completes
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
        sort(values, weights, nextIndex);
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
     * Simple pass that merges entries with the same value
     */
    private static int mergeSameBuckets(double[] values, double[] weights, int nextIndex)
    {
        sort(values, weights, nextIndex);

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
     * The inputs must be sorted by "value" in increasing order
     * The last bucket has a penalty of infinity
     * Entries are doubly-linked to keep track of the relative position of each bucket
     */
    private static PriorityQueue<Entry> initializeQueue(double[] values, double[] weights, int nextIndex)
    {
        checkArgument(nextIndex > 0, "nextIndex must be > 0");

        PriorityQueue<Entry> queue = new PriorityQueue<>(nextIndex);

        Entry right = new Entry(nextIndex - 1, values[nextIndex - 1], weights[nextIndex - 1], null);
        queue.add(right);
        for (int i = nextIndex - 2; i >= 0; i--) {
            Entry current = new Entry(i, values[i], weights[i], right);
            queue.add(current);
            right = current;
        }

        return queue;
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

    private static double computePenalty(double value1, double value2, double weight1, double weight2)
    {
        double weight = value2 + weight2;
        double squaredDifference = (value1 - weight1) * (value1 - weight1);
        double proportionsProduct = (value2 * weight2) / ((value2 + weight2) * (value2 + weight2));
        return weight * squaredDifference * proportionsProduct;
    }

    private static class Entry
            implements Comparable<Entry>
    {
        private final double penalty;

        private final int id;
        private final double value;
        private final double weight;

        private boolean valid = true;
        private Entry left;
        private Entry right;

        private Entry(int id, double value, double weight, Entry right)
        {
            this(id, value, weight, null, right);
        }

        private Entry(int id, double value, double weight, Entry left, Entry right)
        {
            this.id = id;
            this.value = value;
            this.weight = weight;
            this.right = right;
            this.left = left;

            if (right != null) {
                right.left = this;
                penalty = computePenalty(value, weight, right.value, right.weight);
            }
            else {
                penalty = Double.POSITIVE_INFINITY;
            }

            if (left != null) {
                left.right = this;
            }
        }

        public int getId()
        {
            return id;
        }

        public Entry getLeft()
        {
            return left;
        }

        public Entry getRight()
        {
            return right;
        }

        public double getValue()
        {
            return value;
        }

        public double getWeight()
        {
            return weight;
        }

        public boolean isValid()
        {
            return valid;
        }

        public void invalidate()
        {
            this.valid = false;
        }

        @Override
        public int compareTo(Entry other)
        {
            int result = Double.compare(penalty, other.penalty);
            if (result == 0) {
                result = Integer.compare(id, other.id);
            }
            return result;
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("id", id)
                    .add("value", value)
                    .add("weight", weight)
                    .add("penalty", penalty)
                    .add("valid", valid)
                    .toString();
        }
    }
}
