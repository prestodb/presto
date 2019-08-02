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
package com.facebook.presto.facebook.qdigest;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.AtomicDouble;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.facebook.presto.facebook.qdigest.QuantileDigest.MiddleFunction.DEFAULT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * Implements http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.132.7343, a data structure
 * for approximating quantiles by trading off error with memory requirements.
 * <p>
 * The size of the digest is adjusted dynamically to achieve the error bound and requires
 * O(log2(U) / maxError) space, where <em>U</em> is the number of bits needed to represent the
 * domain of the values added to the digest. The error is defined as the discrepancy between the
 * real rank of the value returned in a quantile query and the rank corresponding to the queried
 * quantile.
 * <p>
 * Thus, for a query for quantile <em>q</em> that returns value <em>v</em>, the error is
 * |rank(v) - q * N| / N, where N is the number of elements added to the digest and rank(v) is the
 * real rank of <em>v</em>
 * <p>
 * This class also supports exponential decay. The implementation is based on the ideas laid out
 * in http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.159.3978
 */
@NotThreadSafe
public class QuantileDigest
{
    private static final int MAX_BITS = 64;
    private static final int QUANTILE_DIGEST_SIZE = ClassLayout.parseClass(QuantileDigest.class).instanceSize();

    // needs to be such that Math.exp(alpha * seconds) does not grow too big
    static final long RESCALE_THRESHOLD_SECONDS = 50;
    static final double ZERO_WEIGHT_THRESHOLD = 1e-5;

    private static final int INITIAL_CAPACITY = 1;

    private final double maxError;
    private final Ticker ticker;
    private final double alpha;
    private long landmarkInSeconds;

    private double weightedCount;
    private long max = Long.MIN_VALUE;
    private long min = Long.MAX_VALUE;

    private int root = -1;
    private int nextNode;
    private double[] counts;
    private byte[] levels;
    private long[] values;

    private int[] lefts;
    private int[] rights;

    // We use lefts[] to store a linked list of free slots.
    // freeIndex points to the first available slot
    private int freeCount;
    private int firstFree = -1;

    private enum TraversalOrder
    {
        FORWARD, REVERSE
    }

    /**
     * <p>Create a QuantileDigest with a maximum error guarantee of "maxError" and no decay.
     *
     * @param maxError the max error tolerance
     */
    public QuantileDigest(double maxError)
    {
        this(maxError, 0.0);
    }

    /**
     * <p>Create a QuantileDigest with a maximum error guarantee of "maxError" and exponential decay
     * with factor "alpha".</p>
     *
     * @param maxError the max error tolerance
     * @param alpha the exponential decay factor
     */
    public QuantileDigest(double maxError, double alpha)
    {
        this(maxError, alpha, alpha == 0.0 ? noOpTicker() : Ticker.systemTicker());
    }

    @VisibleForTesting
    QuantileDigest(double maxError, double alpha, Ticker ticker)
    {
        checkArgument(maxError >= 0 && maxError <= 1, "maxError must be in range [0, 1]");
        checkArgument(alpha >= 0 && alpha < 1, "alpha must be in range [0, 1)");

        this.maxError = maxError;
        this.alpha = alpha;
        this.ticker = ticker;

        landmarkInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());

        counts = new double[INITIAL_CAPACITY];
        levels = new byte[INITIAL_CAPACITY];
        values = new long[INITIAL_CAPACITY];

        lefts = new int[INITIAL_CAPACITY];
        rights = new int[INITIAL_CAPACITY];

        Arrays.fill(lefts, -1);
        Arrays.fill(rights, -1);
    }

    public QuantileDigest(QuantileDigest other)
    {
        this.maxError = other.maxError;
        this.alpha = other.alpha;
        this.ticker = alpha == 0.0 ? noOpTicker() : Ticker.systemTicker();

        this.landmarkInSeconds = other.landmarkInSeconds;
        this.weightedCount = other.weightedCount;

        this.max = other.max;
        this.min = other.min;
        this.root = other.root;
        this.nextNode = other.nextNode;
        this.counts = other.counts.clone();
        this.levels = other.levels.clone();
        this.values = other.values.clone();
        this.lefts = other.lefts.clone();
        this.rights = other.rights.clone();
        this.freeCount = other.freeCount;
        this.firstFree = other.firstFree;
    }

    public QuantileDigest(Slice serialized)
    {
        SliceInput input = new BasicSliceInput(serialized);

        byte format = input.readByte();
        checkArgument(format == 0, "Invalid format");
        maxError = input.readDouble();
        alpha = input.readDouble();

        if (alpha == 0.0) {
            ticker = noOpTicker();
        }
        else {
            ticker = Ticker.systemTicker();
        }
        landmarkInSeconds = input.readLong();

        min = input.readLong();
        max = input.readLong();
        int nodeCount = input.readInt();

        int height = MAX_BITS - Long.numberOfLeadingZeros(min ^ max) + 1;
        checkArgument(height >= 64 || nodeCount <= (1L << height) - 1, "Too many nodes in deserialized tree. Possible corruption");

        counts = new double[nodeCount];
        levels = new byte[nodeCount];
        values = new long[nodeCount];

        int[] stack = new int[(Integer.highestOneBit(nodeCount - 1) << 1) + 1]; // log2 ceiling
        int top = -1;

        // The nodes are organized in a left-to-right post-order sequence, so
        // we rebuild the tree (left/right links) bottom up
        lefts = new int[nodeCount];
        rights = new int[nodeCount];
        for (int node = 0; node < nodeCount; node++) {
            byte nodeStructure = input.readByte();
            boolean hasRight = (nodeStructure & Flags.HAS_RIGHT) != 0;
            boolean hasLeft = (nodeStructure & Flags.HAS_LEFT) != 0;
            byte level = (byte) ((nodeStructure >>> 2) & 0b111111);

            // Branch node levels are serialized as 0-indexed to save a bit, therefore if this is not a leaf node then
            // add back one to the level.
            if (hasLeft || hasRight) {
                level++;
            }
            levels[node] = level;

            if (hasRight) {
                rights[node] = stack[top--];
            }
            else {
                rights[node] = -1;
            }

            if (hasLeft) {
                lefts[node] = stack[top--];
            }
            else {
                lefts[node] = -1;
            }

            stack[++top] = node;

            double count = input.readDouble();
            weightedCount += count;
            counts[node] = count;
            values[node] = input.readLong();
        }
        checkArgument(nodeCount == 0 || top == 0, "Tree is corrupted. Expected a single root node");
        root = nodeCount - 1; // last node in post-order

        nextNode = nodeCount;
    }

    public double getMaxError()
    {
        return maxError;
    }

    public double getAlpha()
    {
        return alpha;
    }

    public void add(long value)
    {
        add(value, 1);
    }

    /**
     * Adds a value to this digest. The value must be {@code >= 0}
     */
    public void add(long value, double weight)
    {
        checkArgument(weight > 0, "weight must be > 0");

        boolean needsCompression = false;
        if (alpha > 0.0) {
            long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());
            if (nowInSeconds - landmarkInSeconds >= RESCALE_THRESHOLD_SECONDS) {
                rescale(nowInSeconds);
                needsCompression = true; // rescale affects weights globally, so force compression
            }

            weight *= weight(nowInSeconds);
        }

        max = Math.max(max, value);
        min = Math.min(min, value);

        double previousCount = weightedCount;
        insert(longToBits(value), weight);

        // When totalCount crosses the next multiple of k (compression factor), the compression
        // equation changes for every node in the tree, so we need to compress globally.
        // Otherwise, only node along the insertion path are affected -- TODO: implement this.
        int compressionFactor = calculateCompressionFactor();
        if (needsCompression || ((long) previousCount) / compressionFactor != ((long) weightedCount) / compressionFactor) {
            compress();
        }
    }

    public void add(long value, long weight)
    {
        add(value, (double) weight);
    }

    public void merge(QuantileDigest other)
    {
        rescaleToCommonLandmark(this, other);

        // 1. merge other into this (don't modify other)
        root = merge(root, other, other.root);

        max = Math.max(max, other.max);
        min = Math.min(min, other.min);

        // 2. compress to remove unnecessary nodes
        compress();
    }

    /**
     * Get a lower bound on the quantiles for the given proportions. A returned q quantile is guaranteed to be within
     * the q - maxError and q quantiles.
     * <p>
     * The input list of quantile proportions must be sorted in increasing order, and each value must be in the range [0, 1]
     */
    public List<Long> getQuantilesLowerBound(List<Double> quantiles)
    {
        checkArgument(Ordering.natural().isOrdered(quantiles), "quantiles must be sorted in increasing order");
        for (double quantile : quantiles) {
            checkArgument(quantile >= 0 && quantile <= 1, "quantile must be between [0,1]");
        }

        List<Double> reversedQuantiles = ImmutableList.copyOf(quantiles).reverse();

        ImmutableList.Builder<Long> builder = ImmutableList.builder();
        PeekingIterator<Double> iterator = Iterators.peekingIterator(reversedQuantiles.iterator());

        postOrderTraversal(root, new Callback()
        {
            private double sum;

            @Override
            public boolean process(int node)
            {
                sum += counts[node];

                while (iterator.hasNext() && sum > (1.0 - iterator.peek()) * weightedCount) {
                    iterator.next();

                    // we know the min value ever seen, so cap the percentile to provide better error
                    // bounds in this case
                    long value = Math.max(lowerBound(node), min);

                    builder.add(value);
                }

                return iterator.hasNext();
            }
        }, TraversalOrder.REVERSE);

        // we finished the traversal without consuming all quantiles. This means the remaining quantiles
        // correspond to the max known value
        while (iterator.hasNext()) {
            builder.add(min);
            iterator.next();
        }

        return builder.build().reverse();
    }

    /**
     * Get an upper bound on the quantiles for the given proportions. A returned q quantile is guaranteed to be within
     * the q and q + maxError quantiles.
     * <p>
     * The input list of quantile proportions must be sorted in increasing order, and each value must be in the range [0, 1]
     */
    public List<Long> getQuantilesUpperBound(List<Double> quantiles)
    {
        checkArgument(Ordering.natural().isOrdered(quantiles), "quantiles must be sorted in increasing order");
        for (double quantile : quantiles) {
            checkArgument(quantile >= 0 && quantile <= 1, "quantile must be between [0,1]");
        }

        final ImmutableList.Builder<Long> builder = ImmutableList.builder();
        final PeekingIterator<Double> iterator = Iterators.peekingIterator(quantiles.iterator());

        postOrderTraversal(root, new Callback()
        {
            private double sum;

            public boolean process(int node)
            {
                sum += counts[node];

                while (iterator.hasNext() && sum > iterator.peek() * weightedCount) {
                    iterator.next();

                    // we know the max value ever seen, so cap the percentile to provide better error
                    // bounds in this case
                    long value = Math.min(upperBound(node), max);

                    builder.add(value);
                }

                return iterator.hasNext();
            }
        });

        // we finished the traversal without consuming all quantiles. This means the remaining quantiles
        // correspond to the max known value
        while (iterator.hasNext()) {
            builder.add(max);
            iterator.next();
        }

        return builder.build();
    }

    public List<Long> getQuantiles(List<Double> quantiles)
    {
        return getQuantilesUpperBound(quantiles);
    }

    /**
     * Gets the value at the specified quantile +/- maxError. The quantile must be in the range [0, 1]
     */
    public long getQuantile(double quantile)
    {
        return getQuantiles(ImmutableList.of(quantile)).get(0);
    }

    public long getQuantileLowerBound(double quantile)
    {
        return getQuantilesLowerBound(ImmutableList.of(quantile)).get(0);
    }

    public long getQuantileUpperBound(double quantile)
    {
        return getQuantilesUpperBound(ImmutableList.of(quantile)).get(0);
    }

    /**
     * Number (decayed) of elements added to this quantile digest
     */
    public double getCount()
    {
        return weightedCount / weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read()));
    }

    /*
     * Get the exponentially-decayed approximate counts of values in multiple buckets. The elements in
     * the provided list denote the upper bound each of the buckets and must be sorted in ascending
     * order.
     *
     * The approximate count in each bucket is guaranteed to be within 2 * totalCount * maxError of
     * the real count.
     */
    public List<Bucket> getHistogram(List<Long> bucketUpperBounds)
    {
        return getHistogram(bucketUpperBounds, DEFAULT);
    }

    // A separate lambda is provided to allow one to override how the middle between two quantiles buckets
    // is calculated.
    public List<Bucket> getHistogram(List<Long> bucketUpperBounds, MiddleFunction middleFunction)
    {
        checkArgument(Ordering.natural().isOrdered(bucketUpperBounds), "buckets must be sorted in increasing order");

        ImmutableList.Builder<Bucket> builder = ImmutableList.builder();
        PeekingIterator<Long> iterator = Iterators.peekingIterator(bucketUpperBounds.iterator());

        HistogramBuilderStateHolder holder = new HistogramBuilderStateHolder();

        double normalizationFactor = weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read()));

        postOrderTraversal(root, node -> {
            while (iterator.hasNext() && iterator.peek() <= upperBound(node)) {
                double bucketCount = holder.sum - holder.lastSum;

                Bucket bucket = new Bucket(bucketCount / normalizationFactor, holder.bucketWeightedSum / bucketCount);

                builder.add(bucket);
                holder.lastSum = holder.sum;
                holder.bucketWeightedSum = 0;
                iterator.next();
            }

            holder.bucketWeightedSum += middleFunction.middle(lowerBound(node), upperBound(node)) * counts[node];
            holder.sum += counts[node];
            return iterator.hasNext();
        });

        while (iterator.hasNext()) {
            double bucketCount = holder.sum - holder.lastSum;
            Bucket bucket = new Bucket(bucketCount / normalizationFactor, holder.bucketWeightedSum / bucketCount);

            builder.add(bucket);

            iterator.next();
        }

        return builder.build();
    }

    private static final class HistogramBuilderStateHolder
    {
        double sum;
        double lastSum;
        // for computing weighed average of values in bucket
        double bucketWeightedSum;
    }

    public long getMin()
    {
        final AtomicLong chosen = new AtomicLong(min);
        postOrderTraversal(root, node -> {
            if (counts[node] >= ZERO_WEIGHT_THRESHOLD) {
                chosen.set(lowerBound(node));
                return false;
            }
            return true;
        }, TraversalOrder.FORWARD);

        return Math.max(min, chosen.get());
    }

    public long getMax()
    {
        final AtomicLong chosen = new AtomicLong(max);
        postOrderTraversal(root, node -> {
            if (counts[node] >= ZERO_WEIGHT_THRESHOLD) {
                chosen.set(upperBound(node));
                return false;
            }
            return true;
        }, TraversalOrder.REVERSE);

        return Math.min(max, chosen.get());
    }

    public int estimatedInMemorySizeInBytes()
    {
        return (int) (QUANTILE_DIGEST_SIZE +
                SizeOf.sizeOf(counts) +
                SizeOf.sizeOf(levels) +
                SizeOf.sizeOf(values) +
                SizeOf.sizeOf(lefts) +
                SizeOf.sizeOf(rights));
    }

    public int estimatedSerializedSizeInBytes()
    {
        int nodeSize = SizeOf.SIZE_OF_LONG + // counts
                SizeOf.SIZE_OF_BYTE + // levels and left/right flags
                SizeOf.SIZE_OF_LONG; // values

        return SizeOf.SIZE_OF_BYTE + // format
                SizeOf.SIZE_OF_DOUBLE + // maxError
                SizeOf.SIZE_OF_DOUBLE + // alpha
                SizeOf.SIZE_OF_LONG + // landmarkInSeconds
                SizeOf.SIZE_OF_LONG + // min
                SizeOf.SIZE_OF_LONG + // max
                SizeOf.SIZE_OF_INT + // node count
                getNodeCount() * nodeSize;
    }

    public Slice serialize()
    {
        compress();

        SliceOutput output = new DynamicSliceOutput(estimatedSerializedSizeInBytes());

        output.writeByte(Flags.FORMAT);
        output.writeDouble(maxError);
        output.writeDouble(alpha);
        output.writeLong(landmarkInSeconds);
        output.writeLong(min);
        output.writeLong(max);
        output.writeInt(getNodeCount());

        int[] nodes = new int[getNodeCount()];
        postOrderTraversal(root, new Callback()
        {
            int index;

            @Override
            public boolean process(int node)
            {
                nodes[index++] = node;
                return true;
            }
        });

        for (int node : nodes) {
            // The max value for a level is 64.  Non-leaf nodes are decremented by 1
            // to save a bit (so max serialized value is 63 (111111, 6 bits needed)).
            // This is shifted 2 bits to give space for left/right child flags.
            byte nodeStructure = (byte) (Math.max(levels[node] - 1, 0) << 2);
            if (lefts[node] != -1) {
                nodeStructure |= Flags.HAS_LEFT;
            }
            if (rights[node] != -1) {
                nodeStructure |= Flags.HAS_RIGHT;
            }
            output.writeByte(nodeStructure);
            output.writeDouble(counts[node]);
            output.writeLong(values[node]);
        }

        return output.slice();
    }

    @VisibleForTesting
    int getNodeCount()
    {
        return nextNode - freeCount;
    }

    @VisibleForTesting
    void compress()
    {
        double bound = Math.floor(weightedCount / calculateCompressionFactor());

        postOrderTraversal(root, node -> {
            // if children's weights are 0 remove them and shift the weight to their parent
            int left = lefts[node];
            int right = rights[node];

            if (left == -1 && right == -1) {
                // leaf, nothing to do
                return true;
            }

            double leftCount = (left == -1) ? 0.0 : counts[left];
            double rightCount = (right == -1) ? 0.0 : counts[right];

            boolean shouldCompress = (counts[node] + leftCount + rightCount) < bound;

            if (left != -1 && (shouldCompress || leftCount < ZERO_WEIGHT_THRESHOLD)) {
                lefts[node] = tryRemove(left);
                counts[node] += leftCount;
            }

            if (right != -1 && (shouldCompress || rightCount < ZERO_WEIGHT_THRESHOLD)) {
                rights[node] = tryRemove(right);
                counts[node] += rightCount;
            }

            return true;
        });

        // root's count may have decayed to ~0
        if (root != -1 && counts[root] < ZERO_WEIGHT_THRESHOLD) {
            root = tryRemove(root);
        }
    }

    private double weight(long timestamp)
    {
        return Math.exp(alpha * (timestamp - landmarkInSeconds));
    }

    private void rescale(long newLandmarkInSeconds)
    {
        // rescale the weights based on a new landmark to avoid numerical overflow issues
        double factor = Math.exp(-alpha * (newLandmarkInSeconds - landmarkInSeconds));
        weightedCount *= factor;
        for (int i = 0; i < nextNode; i++) {
            counts[i] *= factor;
        }
        landmarkInSeconds = newLandmarkInSeconds;
    }

    private int calculateCompressionFactor()
    {
        if (root == -1) {
            return 1;
        }

        return Math.max((int) ((levels[root] + 1) / maxError), 1);
    }

    private void insert(long value, double count)
    {
        if (count < ZERO_WEIGHT_THRESHOLD) {
            return;
        }

        long lastBranch = 0;
        int parent = -1;
        int current = root;

        while (true) {
            if (current == -1) {
                setChild(parent, lastBranch, createLeaf(value, count));
                return;
            }

            long currentValue = values[current];
            byte currentLevel = levels[current];
            if (!inSameSubtree(value, currentValue, currentLevel)) {
                // if value and node.value are not in the same branch given node's level,
                // insert a parent above them at the point at which branches diverge
                setChild(parent, lastBranch, makeSiblings(current, createLeaf(value, count)));
                return;
            }

            if (currentLevel == 0 && currentValue == value) {
                // found the node
                counts[current] += count;
                weightedCount += count;
                return;
            }

            // we're on the correct branch of the tree and we haven't reached a leaf, so keep going down
            long branch = value & getBranchMask(currentLevel);

            parent = current;
            lastBranch = branch;

            if (branch == 0) {
                current = lefts[current];
            }
            else {
                current = rights[current];
            }
        }
    }

    private void setChild(int parent, long branch, int child)
    {
        if (parent == -1) {
            root = child;
        }
        else if (branch == 0) {
            lefts[parent] = child;
        }
        else {
            rights[parent] = child;
        }
    }

    private int makeSiblings(int first, int second)
    {
        long firstValue = values[first];
        long secondValue = values[second];

        int parentLevel = MAX_BITS - Long.numberOfLeadingZeros(firstValue ^ secondValue);
        int parent = createNode(firstValue, parentLevel, 0);

        // the branch is given by the bit at the level one below parent
        long branch = firstValue & getBranchMask(levels[parent]);

        if (branch == 0) {
            lefts[parent] = first;
            rights[parent] = second;
        }
        else {
            lefts[parent] = second;
            rights[parent] = first;
        }

        return parent;
    }

    private int createLeaf(long value, double count)
    {
        return createNode(value, 0, count);
    }

    private int createNode(long value, int level, double count)
    {
        int node = popFree();

        if (node == -1) {
            if (nextNode == counts.length) {
                // try to double the array, but don't allocate too much to avoid going over the upper bound of nodes
                // by a large margin (hence, the heuristic to not allocate more than k / 5 nodes)
                int newSize = counts.length + Math.min(counts.length, calculateCompressionFactor() / 5 + 1);
                counts = Arrays.copyOf(counts, newSize);
                levels = Arrays.copyOf(levels, newSize);
                values = Arrays.copyOf(values, newSize);

                lefts = Arrays.copyOf(lefts, newSize);
                rights = Arrays.copyOf(rights, newSize);
            }

            node = nextNode;
            nextNode++;
        }

        weightedCount += count;

        values[node] = value;
        levels[node] = (byte) level;
        counts[node] = count;

        lefts[node] = -1;
        rights[node] = -1;

        return node;
    }

    private int merge(int node, QuantileDigest other, int otherNode)
    {
        if (otherNode == -1) {
            return node;
        }
        else if (node == -1) {
            return copyRecursive(other, otherNode);
        }
        else if (!inSameSubtree(values[node], other.values[otherNode], Math.max(levels[node], other.levels[otherNode]))) {
            return makeSiblings(node, copyRecursive(other, otherNode));
        }
        else if (levels[node] > other.levels[otherNode]) {
            long branch = other.values[otherNode] & getBranchMask(levels[node]);

            if (branch == 0) {
                // variable needed because the array may be re-allocated during merge()
                int left = merge(lefts[node], other, otherNode);
                lefts[node] = left;
            }
            else {
                // variable needed because the array may be re-allocated during merge()
                int right = merge(rights[node], other, otherNode);
                rights[node] = right;
            }
            return node;
        }
        else if (levels[node] < other.levels[otherNode]) {
            long branch = values[node] & getBranchMask(other.levels[otherNode]);

            // variables needed because the arrays may be re-allocated during merge()
            int left;
            int right;
            if (branch == 0) {
                left = merge(node, other, other.lefts[otherNode]);
                right = copyRecursive(other, other.rights[otherNode]);
            }
            else {
                left = copyRecursive(other, other.lefts[otherNode]);
                right = merge(node, other, other.rights[otherNode]);
            }

            int result = createNode(other.values[otherNode], other.levels[otherNode], other.counts[otherNode]);
            lefts[result] = left;
            rights[result] = right;

            return result;
        }

        // else, they must be at the same level and on the same path, so just bump the counts
        weightedCount += other.counts[otherNode];
        counts[node] += other.counts[otherNode];

        // variables needed because the arrays may be re-allocated during merge()
        int left = merge(lefts[node], other, other.lefts[otherNode]);
        int right = merge(rights[node], other, other.rights[otherNode]);
        lefts[node] = left;
        rights[node] = right;

        return node;
    }

    private static boolean inSameSubtree(long bitsA, long bitsB, int level)
    {
        return level == MAX_BITS || (bitsA >>> level) == (bitsB >>> level);
    }

    private int copyRecursive(QuantileDigest other, int otherNode)
    {
        if (otherNode == -1) {
            return otherNode;
        }

        int node = createNode(other.values[otherNode], other.levels[otherNode], other.counts[otherNode]);

        if (other.lefts[otherNode] != -1) {
            // variable needed because the array may be re-allocated during merge()
            int left = copyRecursive(other, other.lefts[otherNode]);
            lefts[node] = left;
        }

        if (other.rights[otherNode] != -1) {
            // variable needed because the array may be re-allocated during merge()
            int right = copyRecursive(other, other.rights[otherNode]);
            rights[node] = right;
        }

        return node;
    }

    /**
     * Remove the node if possible or set its count to 0 if it has children and
     * it needs to be kept around
     */
    private int tryRemove(int node)
    {
        checkArgument(node != -1, "node is -1");

        int left = lefts[node];
        int right = rights[node];

        if (left == -1 && right == -1) {
            // leaf, just remove it
            remove(node);
            return -1;
        }

        if (left != -1 && right != -1) {
            // node has both children so we can't physically remove it
            counts[node] = 0;
            return node;
        }

        // node has a single child, so remove it and return the child
        remove(node);
        if (left != -1) {
            return left;
        }
        else {
            return right;
        }
    }

    private void remove(int node)
    {
        if (node == nextNode - 1) {
            // if we're removing the last node, no need to add it to the free list
            nextNode--;
        }
        else {
            pushFree(node);
        }

        if (node == root) {
            root = -1;
        }
    }

    private void pushFree(int node)
    {
        lefts[node] = firstFree;
        firstFree = node;
        freeCount++;
    }

    private int popFree()
    {
        int node = firstFree;

        if (node == -1) {
            return node;
        }

        firstFree = lefts[firstFree];
        freeCount--;

        return node;
    }

    private void postOrderTraversal(int node, Callback callback)
    {
        postOrderTraversal(node, callback, TraversalOrder.FORWARD);
    }

    private void postOrderTraversal(int node, Callback callback, TraversalOrder order)
    {
        if (order == TraversalOrder.FORWARD) {
            postOrderTraversal(node, callback, lefts, rights);
        }
        else {
            postOrderTraversal(node, callback, rights, lefts);
        }
    }

    private boolean postOrderTraversal(int node, Callback callback, int[] lefts, int[] rights)
    {
        if (node == -1) {
            return false;
        }

        int first = lefts[node];
        int second = rights[node];

        if (first != -1 && !postOrderTraversal(first, callback, lefts, rights)) {
            return false;
        }

        if (second != -1 && !postOrderTraversal(second, callback, lefts, rights)) {
            return false;
        }

        return callback.process(node);
    }

    /**
     * Computes the maximum error of the current digest
     */
    public double getConfidenceFactor()
    {
        return computeMaxPathWeight(root) * 1.0 / weightedCount;
    }

    @VisibleForTesting
    boolean equivalent(QuantileDigest other)
    {
        return (getNodeCount() == other.getNodeCount() &&
                min == other.min &&
                max == other.max &&
                weightedCount == other.weightedCount &&
                alpha == other.alpha);
    }

    private void rescaleToCommonLandmark(QuantileDigest one, QuantileDigest two)
    {
        long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());

        // 1. rescale this and other to common landmark
        long targetLandmark = Math.max(one.landmarkInSeconds, two.landmarkInSeconds);

        if (nowInSeconds - targetLandmark >= RESCALE_THRESHOLD_SECONDS) {
            targetLandmark = nowInSeconds;
        }

        if (targetLandmark != one.landmarkInSeconds) {
            one.rescale(targetLandmark);
        }

        if (targetLandmark != two.landmarkInSeconds) {
            two.rescale(targetLandmark);
        }
    }

    /**
     * Computes the max "weight" of any path starting at node and ending at a leaf in the
     * hypothetical complete tree. The weight is the sum of counts in the ancestors of a given node
     */
    private double computeMaxPathWeight(int node)
    {
        if (node == -1 || levels[node] == 0) {
            return 0;
        }

        double leftMaxWeight = computeMaxPathWeight(lefts[node]);
        double rightMaxWeight = computeMaxPathWeight(rights[node]);

        return Math.max(leftMaxWeight, rightMaxWeight) + counts[node];
    }

    @VisibleForTesting
    void validate()
    {
        AtomicDouble sum = new AtomicDouble();
        AtomicInteger nodeCount = new AtomicInteger();

        Set<Integer> freeSlots = computeFreeList();
        checkState(freeSlots.size() == freeCount, "Free count (%s) doesn't match actual free slots: %s", freeCount, freeSlots.size());

        if (root != -1) {
            validateStructure(root, freeSlots);

            postOrderTraversal(root, node -> {
                sum.addAndGet(counts[node]);
                nodeCount.incrementAndGet();
                return true;
            });
        }

        checkState(Math.abs(sum.get() - weightedCount) < ZERO_WEIGHT_THRESHOLD,
                "Computed weight (%s) doesn't match summary (%s)", sum.get(),
                weightedCount);

        checkState(nodeCount.get() == getNodeCount(),
                "Actual node count (%s) doesn't match summary (%s)",
                nodeCount.get(), getNodeCount());
    }

    private void validateStructure(int node, Set<Integer> freeNodes)
    {
        checkState(levels[node] >= 0);

        checkState(!freeNodes.contains(node), "Node is in list of free slots: %s", node);
        if (lefts[node] != -1) {
            validateBranchStructure(node, lefts[node], rights[node], true);
            validateStructure(lefts[node], freeNodes);
        }

        if (rights[node] != -1) {
            validateBranchStructure(node, rights[node], lefts[node], false);
            validateStructure(rights[node], freeNodes);
        }
    }

    private void validateBranchStructure(int parent, int child, int otherChild, boolean isLeft)
    {
        checkState(levels[child] < levels[parent], "Child level (%s) should be smaller than parent level (%s)", levels[child], levels[parent]);

        long branch = values[child] & (1L << (levels[parent] - 1));
        checkState(branch == 0 && isLeft || branch != 0 && !isLeft, "Value of child node is inconsistent with its branch");

        Preconditions.checkState(counts[parent] > 0 ||
                        counts[child] > 0 || otherChild != -1,
                "Found a linear chain of zero-weight nodes");
    }

    private Set<Integer> computeFreeList()
    {
        Set<Integer> freeSlots = new HashSet<>();
        int index = firstFree;
        while (index != -1) {
            freeSlots.add(index);
            index = lefts[index];
        }
        return freeSlots;
    }

    public String toGraphviz()
    {
        StringBuilder builder = new StringBuilder();

        builder.append("digraph QuantileDigest {\n")
                .append("\tgraph [ordering=\"out\"];");

        final List<Integer> nodes = new ArrayList<>();
        postOrderTraversal(root, node -> {
            nodes.add(node);
            return true;
        });

        Multimap<Byte, Integer> nodesByLevel = Multimaps.index(nodes, input -> levels[input]);

        for (Map.Entry<Byte, Collection<Integer>> entry : nodesByLevel.asMap().entrySet()) {
            builder.append("\tsubgraph level_" + entry.getKey() + " {\n")
                    .append("\t\trank = same;\n");

            for (int node : entry.getValue()) {
                if (levels[node] == 0) {
                    builder.append(String.format("\t\t%s [label=\"%s:[%s]@%s\\n%s\", shape=rect, style=filled,color=%s];\n",
                            idFor(node),
                            node,
                            lowerBound(node),
                            levels[node],
                            counts[node],
                            counts[node] > 0 ? "salmon2" : "white"));
                }
                else {
                    builder.append(String.format("\t\t%s [label=\"%s:[%s..%s]@%s\\n%s\", shape=rect, style=filled,color=%s];\n",
                            idFor(node),
                            node,
                            lowerBound(node),
                            upperBound(node),
                            levels[node],
                            counts[node],
                            counts[node] > 0 ? "salmon2" : "white"));
                }
            }
            builder.append("\t}\n");
        }

        for (int node : nodes) {
            if (lefts[node] != -1) {
                builder.append(format("\t%s -> %s [style=\"%s\"];\n",
                        idFor(node),
                        idFor(lefts[node]),
                        levels[node] - levels[lefts[node]] == 1 ? "solid" : "dotted"));
            }
            if (rights[node] != -1) {
                builder.append(format("\t%s -> %s [style=\"%s\"];\n",
                        idFor(node),
                        idFor(rights[node]),
                        levels[node] - levels[rights[node]] == 1 ? "solid" : "dotted"));
            }
        }

        builder.append("}\n");

        return builder.toString();
    }

    private static String idFor(int node)
    {
        return String.format("node_%x", node);
    }

    /**
     * Convert a java long (two's complement representation) to a 64-bit lexicographically-sortable binary
     */
    private static long longToBits(long value)
    {
        return value ^ 0x8000_0000_0000_0000L;
    }

    /**
     * Convert a 64-bit lexicographically-sortable binary to a java long (two's complement representation)
     */
    private static long bitsToLong(long bits)
    {
        return bits ^ 0x8000_0000_0000_0000L;
    }

    private long getBranchMask(byte level)
    {
        return (1L << (level - 1));
    }

    private long upperBound(int node)
    {
        // set all lsb below level to 1 (we're looking for the highest value of the range covered by this node)
        long mask = 0;

        if (levels[node] > 0) { // need to special case when level == 0 because (value >> 64 really means value >> (64 % 64))
            mask = 0xFFFF_FFFF_FFFF_FFFFL >>> (MAX_BITS - levels[node]);
        }
        return bitsToLong(values[node] | mask);
    }

    private long lowerBound(int node)
    {
        // set all lsb below level to 0 (we're looking for the lowest value of the range covered by this node)
        long mask = 0;

        if (levels[node] > 0) { // need to special case when level == 0 because (value >> 64 really means value >> (64 % 64))
            mask = 0xFFFF_FFFF_FFFF_FFFFL >>> (MAX_BITS - levels[node]);
        }

        return bitsToLong(values[node] & (~mask));
    }

    private long middle(int node)
    {
        long lower = lowerBound(node);
        long upper = upperBound(node);

        return lower + (upper - lower) / 2;
    }

    private static Ticker noOpTicker()
    {
        return new Ticker()
        {
            @Override
            public long read()
            {
                return 0;
            }
        };
    }

    public static class Bucket
    {
        private double count;
        private double mean;

        public Bucket(double count, double mean)
        {
            this.count = count;
            this.mean = mean;
        }

        public double getCount()
        {
            return count;
        }

        public double getMean()
        {
            return mean;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Bucket bucket = (Bucket) o;

            if (Double.compare(bucket.count, count) != 0) {
                return false;
            }
            if (Double.compare(bucket.mean, mean) != 0) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode()
        {
            int result;
            long temp;
            temp = count != +0.0d ? Double.doubleToLongBits(count) : 0L;
            result = (int) (temp ^ (temp >>> 32));
            temp = mean != +0.0d ? Double.doubleToLongBits(mean) : 0L;
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            return result;
        }

        public String toString()
        {
            return String.format("[count: %f, mean: %f]", count, mean);
        }
    }

    private interface Callback
    {
        /**
         * @param node the node to process
         * @return true if processing should continue
         */
        boolean process(int node);
    }

    private static class Flags
    {
        public static final int HAS_LEFT = 1 << 0;
        public static final int HAS_RIGHT = 1 << 1;
        public static final byte FORMAT = 0; // Currently there is just one format
    }

    public interface MiddleFunction
    {
        MiddleFunction DEFAULT = (lowerBound, upperBound) -> lowerBound + (upperBound - lowerBound) / 2.0;

        double middle(long lowerBound, long upperBound);
    }
}
