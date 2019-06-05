package com.facebook.presto.operator.scalar;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 This is a copy of the same class in the Airlift library, with the addition of a method to calculate
 the truncated mean. This is intended to be temporary, as we are working on merging in changes to the
  public class to enable us to write the truncated mean method outside of the class body:
  https://github.com/airlift/airlift/issues/710
 */
@NotThreadSafe
public class QuantileDigest
{
    private static final int MAX_BITS = 64;
    private static final int QUANTILE_DIGEST_SIZE = ClassLayout.parseClass(QuantileDigest.class).instanceSize();
    static final long RESCALE_THRESHOLD_SECONDS = 50L;
    static final double ZERO_WEIGHT_THRESHOLD = 1.0E-5D;
    private static final int INITIAL_CAPACITY = 1;
    private final double maxError;
    private final Ticker ticker;
    private final double alpha;
    private long landmarkInSeconds;
    private double weightedCount;
    private long max;
    private long min;
    private int root;
    private int nextNode;
    private double[] counts;
    private byte[] levels;
    private long[] values;
    private int[] lefts;
    private int[] rights;
    private int freeCount;
    private int firstFree;

    public QuantileDigest(double maxError)
    {
        this(maxError, 0.0D);
    }

    public QuantileDigest(double maxError, double alpha)
    {
        this(maxError, alpha, alpha == 0.0D ? noOpTicker() : Ticker.systemTicker());
    }

    @VisibleForTesting
    QuantileDigest(double maxError, double alpha, Ticker ticker)
    {
        this.max = -9223372036854775808L;
        this.min = 9223372036854775807L;
        this.root = -1;
        this.firstFree = -1;
        Preconditions.checkArgument(maxError >= 0.0D && maxError <= 1.0D, "maxError must be in range [0, 1]");
        Preconditions.checkArgument(alpha >= 0.0D && alpha < 1.0D, "alpha must be in range [0, 1)");
        this.maxError = maxError;
        this.alpha = alpha;
        this.ticker = ticker;
        this.landmarkInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());
        this.counts = new double[1];
        this.levels = new byte[1];
        this.values = new long[1];
        this.lefts = new int[1];
        this.rights = new int[1];
        Arrays.fill(this.lefts, -1);
        Arrays.fill(this.rights, -1);
    }

    public QuantileDigest(QuantileDigest other)
    {
        this.max = -9223372036854775808L;
        this.min = 9223372036854775807L;
        this.root = -1;
        this.firstFree = -1;
        this.maxError = other.maxError;
        this.alpha = other.alpha;
        this.ticker = this.alpha == 0.0D ? noOpTicker() : Ticker.systemTicker();
        this.landmarkInSeconds = other.landmarkInSeconds;
        this.weightedCount = other.weightedCount;
        this.max = other.max;
        this.min = other.min;
        this.root = other.root;
        this.nextNode = other.nextNode;
        this.counts = (double[]) other.counts.clone();
        this.levels = (byte[]) other.levels.clone();
        this.values = (long[]) other.values.clone();
        this.lefts = (int[]) other.lefts.clone();
        this.rights = (int[]) other.rights.clone();
        this.freeCount = other.freeCount;
        this.firstFree = other.firstFree;
    }

    public QuantileDigest(Slice serialized)
    {
        this.max = -9223372036854775808L;
        this.min = 9223372036854775807L;
        this.root = -1;
        this.firstFree = -1;
        SliceInput input = new BasicSliceInput(serialized);
        byte format = input.readByte();
        Preconditions.checkArgument(format == 0, "Invalid format");
        this.maxError = input.readDouble();
        this.alpha = input.readDouble();
        if (this.alpha == 0.0D) {
            this.ticker = noOpTicker();
        }
        else {
            this.ticker = Ticker.systemTicker();
        }

        this.landmarkInSeconds = input.readLong();
        this.min = input.readLong();
        this.max = input.readLong();
        int nodeCount = input.readInt();
        int numberOfLevels = 64 - Long.numberOfLeadingZeros(this.min ^ this.max) + 1;
        double k = (double) (3 * numberOfLevels) / this.maxError;
        Preconditions.checkArgument((double) nodeCount <= 2.0D * k, "Too many nodes in deserialized tree. Possible corruption");
        this.counts = new double[nodeCount];
        this.levels = new byte[nodeCount];
        this.values = new long[nodeCount];
        int[] stack = new int[(Integer.highestOneBit(nodeCount - 1) << 1) + 1];
        int top = -1;
        this.lefts = new int[nodeCount];
        this.rights = new int[nodeCount];

        for (int node = 0; node < nodeCount; ++node) {
            byte nodeStructure = input.readByte();
            boolean hasRight = (nodeStructure & 2) != 0;
            boolean hasLeft = (nodeStructure & 1) != 0;
            byte level = (byte) (nodeStructure >>> 2 & 63);
            if (hasLeft || hasRight) {
                ++level;
            }

            this.levels[node] = level;
            if (hasRight) {
                this.rights[node] = stack[top--];
            }
            else {
                this.rights[node] = -1;
            }

            if (hasLeft) {
                this.lefts[node] = stack[top--];
            }
            else {
                this.lefts[node] = -1;
            }

            ++top;
            stack[top] = node;
            double count = input.readDouble();
            this.weightedCount += count;
            this.counts[node] = count;
            this.values[node] = input.readLong();
        }

        Preconditions.checkArgument(nodeCount == 0 || top == 0, "Tree is corrupted. Expected a single root node");
        this.root = nodeCount - 1;
        this.nextNode = nodeCount;
    }

    public double getMaxError()
    {
        return this.maxError;
    }

    public double getAlpha()
    {
        return this.alpha;
    }

    public void add(long value)
    {
        this.add(value, 1L);
    }

    public void add(long value, long count)
    {
        Preconditions.checkArgument(count > 0L, "count must be > 0");
        boolean needsCompression = false;
        double weight = (double) count;
        if (this.alpha > 0.0D) {
            long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(this.ticker.read());
            if (nowInSeconds - this.landmarkInSeconds >= 50L) {
                this.rescale(nowInSeconds);
                needsCompression = true;
            }

            weight = this.weight(nowInSeconds) * (double) count;
        }

        this.max = Math.max(this.max, value);
        this.min = Math.min(this.min, value);
        double previousCount = this.weightedCount;
        this.insert(longToBits(value), weight);
        int compressionFactor = this.calculateCompressionFactor();
        if (needsCompression || (long) previousCount / (long) compressionFactor != (long) this.weightedCount / (long) compressionFactor) {
            this.compress();
        }
    }

    public void merge(QuantileDigest other)
    {
        this.rescaleToCommonLandmark(this, other);
        this.root = this.merge(this.root, other, other.root);
        this.max = Math.max(this.max, other.max);
        this.min = Math.min(this.min, other.min);
        this.compress();
    }

    public List<Long> getQuantilesLowerBound(List<Double> quantiles)
    {
        Preconditions.checkArgument(Ordering.natural().isOrdered(quantiles), "quantiles must be sorted in increasing order");
        Iterator var2 = quantiles.iterator();

        while (var2.hasNext()) {
            double quantile = (Double) var2.next();
            Preconditions.checkArgument(quantile >= 0.0D && quantile <= 1.0D, "quantile must be between [0,1]");
        }

        List<Double> reversedQuantiles = ImmutableList.copyOf(quantiles).reverse();
        final Builder<Long> builder = ImmutableList.builder();
        final PeekingIterator<Double> iterator = Iterators.peekingIterator(reversedQuantiles.iterator());
        this.postOrderTraversal(this.root, new QuantileDigest.Callback()
        {
            private double sum;

            public boolean process(int node)
            {
                this.sum += QuantileDigest.this.counts[node];

                while (iterator.hasNext() && this.sum > (1.0D - (Double) iterator.peek()) * QuantileDigest.this.weightedCount) {
                    iterator.next();
                    long value = Math.max(QuantileDigest.this.lowerBound(node), QuantileDigest.this.min);
                    builder.add(value);
                }

                return iterator.hasNext();
            }
        }, QuantileDigest.TraversalOrder.REVERSE);

        while (iterator.hasNext()) {
            builder.add(this.min);
            iterator.next();
        }

        return builder.build().reverse();
    }

    public List<Long> getQuantilesUpperBound(List<Double> quantiles)
    {
        Preconditions.checkArgument(Ordering.natural().isOrdered(quantiles), "quantiles must be sorted in increasing order");
        Iterator var2 = quantiles.iterator();

        while (var2.hasNext()) {
            double quantile = (Double) var2.next();
            Preconditions.checkArgument(quantile >= 0.0D && quantile <= 1.0D, "quantile must be between [0,1]");
        }

        final Builder<Long> builder = ImmutableList.builder();
        final PeekingIterator<Double> iterator = Iterators.peekingIterator(quantiles.iterator());
        this.postOrderTraversal(this.root, new QuantileDigest.Callback()
        {
            private double sum;

            public boolean process(int node)
            {
                this.sum += QuantileDigest.this.counts[node];

                while (iterator.hasNext() && this.sum > (Double) iterator.peek() * QuantileDigest.this.weightedCount) {
                    iterator.next();
                    long value = Math.min(QuantileDigest.this.upperBound(node), QuantileDigest.this.max);
                    builder.add(value);
                }

                return iterator.hasNext();
            }
        });

        while (iterator.hasNext()) {
            builder.add(this.max);
            iterator.next();
        }

        return builder.build();
    }

    public List<Long> getQuantiles(List<Double> quantiles)
    {
        return this.getQuantilesUpperBound(quantiles);
    }

    public long getQuantile(double quantile)
    {
        return (Long) this.getQuantiles(ImmutableList.of(quantile)).get(0);
    }

    public long getQuantileLowerBound(double quantile)
    {
        return (Long) this.getQuantilesLowerBound(ImmutableList.of(quantile)).get(0);
    }

    public long getQuantileUpperBound(double quantile)
    {
        return (Long) this.getQuantilesUpperBound(ImmutableList.of(quantile)).get(0);
    }

    public double getCount()
    {
        return this.weightedCount / this.weight(TimeUnit.NANOSECONDS.toSeconds(this.ticker.read()));
    }

    public List<QuantileDigest.Bucket> getHistogram(List<Long> bucketUpperBounds)
    {
        return this.getHistogram(bucketUpperBounds, QuantileDigest.MiddleFunction.DEFAULT);
    }

    public List<QuantileDigest.Bucket> getHistogram(List<Long> bucketUpperBounds, QuantileDigest.MiddleFunction middleFunction)
    {
        Preconditions.checkArgument(Ordering.natural().isOrdered(bucketUpperBounds), "buckets must be sorted in increasing order");
        Builder<QuantileDigest.Bucket> builder = ImmutableList.builder();
        PeekingIterator<Long> iterator = Iterators.peekingIterator(bucketUpperBounds.iterator());
        QuantileDigest.HistogramBuilderStateHolder holder = new QuantileDigest.HistogramBuilderStateHolder();
        double normalizationFactor = this.weight(TimeUnit.NANOSECONDS.toSeconds(this.ticker.read()));
        this.postOrderTraversal(this.root, (node) -> {
            while (iterator.hasNext() && (Long) iterator.peek() <= this.upperBound(node)) {
                double bucketCount = holder.sum - holder.lastSum;
                QuantileDigest.Bucket bucket = new QuantileDigest.Bucket(bucketCount / normalizationFactor, holder.bucketWeightedSum / bucketCount);
                builder.add(bucket);
                holder.lastSum = holder.sum;
                holder.bucketWeightedSum = 0.0D;
                iterator.next();
            }

            holder.bucketWeightedSum += middleFunction.middle(this.lowerBound(node), this.upperBound(node)) * this.counts[node];
            holder.sum += this.counts[node];
            return iterator.hasNext();
        });

        while (iterator.hasNext()) {
            double bucketCount = holder.sum - holder.lastSum;
            QuantileDigest.Bucket bucket = new QuantileDigest.Bucket(bucketCount / normalizationFactor, holder.bucketWeightedSum / bucketCount);
            builder.add(bucket);
            iterator.next();
        }

        return builder.build();
    }

    public long getMin()
    {
        AtomicLong chosen = new AtomicLong(this.min);
        this.postOrderTraversal(this.root, (node) -> {
            if (this.counts[node] >= 1.0E-5D) {
                chosen.set(this.lowerBound(node));
                return false;
            }
            else {
                return true;
            }
        }, QuantileDigest.TraversalOrder.FORWARD);
        return Math.max(this.min, chosen.get());
    }

    public long getMax()
    {
        AtomicLong chosen = new AtomicLong(this.max);
        this.postOrderTraversal(this.root, (node) -> {
            if (this.counts[node] >= 1.0E-5D) {
                chosen.set(this.upperBound(node));
                return false;
            }
            else {
                return true;
            }
        }, QuantileDigest.TraversalOrder.REVERSE);
        return Math.min(this.max, chosen.get());
    }

    /**
     * Get the approx truncated mean from the digest.
     * The mean is computed as a weighted average of values between the upper and lower quantiles (inclusive)
     * When the rank of a quantile is non-integer, a portion of the nearest rank's value is included in the mean
     * according to its fraction within the quantile bound.
     * <p>
     * If the value in the digest needs to be converted before the mean is calculated,
     * valueFunction can be used. Else, pass in the identity function.
     */
    public Double getTruncatedMean(double lowerQuantile, double upperQuantile, TruncMeanValueFunction valueFunction)
    {
        if (weightedCount == 0 || lowerQuantile >= upperQuantile) {
            return null;
        }

        AtomicDouble meanResult = new AtomicDouble();
        double lowerRank = lowerQuantile * weightedCount;
        double upperRank = upperQuantile * weightedCount;

        postOrderTraversal(root, new Callback()
        {
            private double sum;
            private double count;
            private double mean;

            public boolean process(int node)
            {
                double nodeCount = counts[node];
                if (nodeCount == 0) {
                    return true;
                }

                sum += nodeCount;

                double amountOverLower = Math.max(sum - lowerRank, 0);
                double amountOverUpper = Math.max(sum - upperRank, 0);
                // Constrain node count to the rank of the lower and upper bound quantiles
                nodeCount = Math.max(0, Math.min(Math.min(nodeCount, amountOverLower), nodeCount - amountOverUpper));

                if (amountOverLower > 0) {
                    double value = valueFunction.value(Math.min(upperBound(node), max));
                    mean = (mean * count + nodeCount * value) / (count + nodeCount);
                    count += nodeCount;
                }

                if (amountOverUpper > 0 || sum == weightedCount) {
                    meanResult.set(mean);
                    return false;
                }
                return true;
            }
        });

        return meanResult.get();
    }

    public long estimatedInMemorySizeInBytes()
    {
        return ((long) QUANTILE_DIGEST_SIZE + SizeOf.sizeOf(this.counts) + SizeOf.sizeOf(this.levels) + SizeOf.sizeOf(this.values) + SizeOf.sizeOf(this.lefts) + SizeOf.sizeOf(this.rights));
    }

    public int estimatedSerializedSizeInBytes()
    {
        int nodeSize = 17;
        return 45 + this.getNodeCount() * nodeSize;
    }

    public Slice serialize()
    {
        this.compress();
        SliceOutput output = new DynamicSliceOutput(this.estimatedSerializedSizeInBytes());
        output.writeByte(0);
        output.writeDouble(this.maxError);
        output.writeDouble(this.alpha);
        output.writeLong(this.landmarkInSeconds);
        output.writeLong(this.min);
        output.writeLong(this.max);
        output.writeInt(this.getNodeCount());
        final int[] nodes = new int[this.getNodeCount()];
        this.postOrderTraversal(this.root, new QuantileDigest.Callback()
        {
            int index;

            public boolean process(int node)
            {
                nodes[this.index++] = node;
                return true;
            }
        });
        int[] var3 = nodes;
        int var4 = nodes.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            int node = var3[var5];
            byte nodeStructure = (byte) (Math.max(this.levels[node] - 1, 0) << 2);
            if (this.lefts[node] != -1) {
                nodeStructure = (byte) (nodeStructure | 1);
            }

            if (this.rights[node] != -1) {
                nodeStructure = (byte) (nodeStructure | 2);
            }

            output.writeByte(nodeStructure);
            output.writeDouble(this.counts[node]);
            output.writeLong(this.values[node]);
        }

        return output.slice();
    }

    @VisibleForTesting
    int getNodeCount()
    {
        return this.nextNode - this.freeCount;
    }

    @VisibleForTesting
    void compress()
    {
        double bound = Math.floor(this.weightedCount / (double) this.calculateCompressionFactor());
        this.postOrderTraversal(this.root, (node) -> {
            int left = this.lefts[node];
            int right = this.rights[node];
            if (left == -1 && right == -1) {
                return true;
            }
            else {
                double leftCount = left == -1 ? 0.0D : this.counts[left];
                double rightCount = right == -1 ? 0.0D : this.counts[right];
                boolean shouldCompress = this.counts[node] + leftCount + rightCount < bound;
                double[] var10000;
                if (left != -1 && (shouldCompress || leftCount < 1.0E-5D)) {
                    this.lefts[node] = this.tryRemove(left);
                    var10000 = this.counts;
                    var10000[node] += leftCount;
                }

                if (right != -1 && (shouldCompress || rightCount < 1.0E-5D)) {
                    this.rights[node] = this.tryRemove(right);
                    var10000 = this.counts;
                    var10000[node] += rightCount;
                }

                return true;
            }
        });
        if (this.root != -1 && this.counts[this.root] < 1.0E-5D) {
            this.root = this.tryRemove(this.root);
        }
    }

    private double weight(long timestamp)
    {
        return Math.exp(this.alpha * (double) (timestamp - this.landmarkInSeconds));
    }

    private void rescale(long newLandmarkInSeconds)
    {
        double factor = Math.exp(-this.alpha * (double) (newLandmarkInSeconds - this.landmarkInSeconds));
        this.weightedCount *= factor;

        for (int i = 0; i < this.nextNode; ++i) {
            double[] var10000 = this.counts;
            var10000[i] *= factor;
        }

        this.landmarkInSeconds = newLandmarkInSeconds;
    }

    private int calculateCompressionFactor()
    {
        return this.root == -1 ? 1 : Math.max((int) ((double) (this.levels[this.root] + 1) / this.maxError), 1);
    }

    private void insert(long value, double count)
    {
        if (count >= 1.0E-5D) {
            long lastBranch = 0L;
            int parent = -1;
            int current = this.root;

            while (current != -1) {
                long currentValue = this.values[current];
                byte currentLevel = this.levels[current];
                if (!inSameSubtree(value, currentValue, currentLevel)) {
                    this.setChild(parent, lastBranch, this.makeSiblings(current, this.createLeaf(value, count)));
                    return;
                }

                if (currentLevel == 0 && currentValue == value) {
                    double[] var10000 = this.counts;
                    var10000[current] += count;
                    this.weightedCount += count;
                    return;
                }

                long branch = value & this.getBranchMask(currentLevel);
                parent = current;
                lastBranch = branch;
                if (branch == 0L) {
                    current = this.lefts[current];
                }
                else {
                    current = this.rights[current];
                }
            }

            this.setChild(parent, lastBranch, this.createLeaf(value, count));
        }
    }

    private void setChild(int parent, long branch, int child)
    {
        if (parent == -1) {
            this.root = child;
        }
        else if (branch == 0L) {
            this.lefts[parent] = child;
        }
        else {
            this.rights[parent] = child;
        }
    }

    private int makeSiblings(int first, int second)
    {
        long firstValue = this.values[first];
        long secondValue = this.values[second];
        int parentLevel = 64 - Long.numberOfLeadingZeros(firstValue ^ secondValue);
        int parent = this.createNode(firstValue, parentLevel, 0.0D);
        long branch = firstValue & this.getBranchMask(this.levels[parent]);
        if (branch == 0L) {
            this.lefts[parent] = first;
            this.rights[parent] = second;
        }
        else {
            this.lefts[parent] = second;
            this.rights[parent] = first;
        }

        return parent;
    }

    private int createLeaf(long value, double count)
    {
        return this.createNode(value, 0, count);
    }

    private int createNode(long value, int level, double count)
    {
        int node = this.popFree();
        if (node == -1) {
            if (this.nextNode == this.counts.length) {
                int newSize = this.counts.length + Math.min(this.counts.length, this.calculateCompressionFactor() / 5 + 1);
                this.counts = Arrays.copyOf(this.counts, newSize);
                this.levels = Arrays.copyOf(this.levels, newSize);
                this.values = Arrays.copyOf(this.values, newSize);
                this.lefts = Arrays.copyOf(this.lefts, newSize);
                this.rights = Arrays.copyOf(this.rights, newSize);
            }

            node = this.nextNode++;
        }

        this.weightedCount += count;
        this.values[node] = value;
        this.levels[node] = (byte) level;
        this.counts[node] = count;
        this.lefts[node] = -1;
        this.rights[node] = -1;
        return node;
    }

    private int merge(int node, QuantileDigest other, int otherNode)
    {
        if (otherNode == -1) {
            return node;
        }
        else if (node == -1) {
            return this.copyRecursive(other, otherNode);
        }
        else if (!inSameSubtree(this.values[node], other.values[otherNode], Math.max(this.levels[node], other.levels[otherNode]))) {
            return this.makeSiblings(node, this.copyRecursive(other, otherNode));
        }
        else {
            int left;
            long branch;
            if (this.levels[node] > other.levels[otherNode]) {
                branch = other.values[otherNode] & this.getBranchMask(this.levels[node]);
                if (branch == 0L) {
                    left = this.merge(this.lefts[node], other, otherNode);
                    this.lefts[node] = left;
                }
                else {
                    left = this.merge(this.rights[node], other, otherNode);
                    this.rights[node] = left;
                }

                return node;
            }
            else if (this.levels[node] < other.levels[otherNode]) {
                branch = this.values[node] & this.getBranchMask(other.levels[otherNode]);
                int right;
                if (branch == 0L) {
                    left = this.merge(node, other, other.lefts[otherNode]);
                    right = this.copyRecursive(other, other.rights[otherNode]);
                }
                else {
                    left = this.copyRecursive(other, other.lefts[otherNode]);
                    right = this.merge(node, other, other.rights[otherNode]);
                }

                int result = this.createNode(other.values[otherNode], other.levels[otherNode], other.counts[otherNode]);
                this.lefts[result] = left;
                this.rights[result] = right;
                return result;
            }
            else {
                this.weightedCount += other.counts[otherNode];
                double[] var10000 = this.counts;
                var10000[node] += other.counts[otherNode];
                left = this.merge(this.lefts[node], other, other.lefts[otherNode]);
                int right = this.merge(this.rights[node], other, other.rights[otherNode]);
                this.lefts[node] = left;
                this.rights[node] = right;
                return node;
            }
        }
    }

    private static boolean inSameSubtree(long bitsA, long bitsB, int level)
    {
        return level == 64 || bitsA >>> level == bitsB >>> level;
    }

    private int copyRecursive(QuantileDigest other, int otherNode)
    {
        if (otherNode == -1) {
            return otherNode;
        }
        else {
            int node = this.createNode(other.values[otherNode], other.levels[otherNode], other.counts[otherNode]);
            int right;
            if (other.lefts[otherNode] != -1) {
                right = this.copyRecursive(other, other.lefts[otherNode]);
                this.lefts[node] = right;
            }

            if (other.rights[otherNode] != -1) {
                right = this.copyRecursive(other, other.rights[otherNode]);
                this.rights[node] = right;
            }

            return node;
        }
    }

    private int tryRemove(int node)
    {
        Preconditions.checkArgument(node != -1, "node is -1");
        int left = this.lefts[node];
        int right = this.rights[node];
        if (left == -1 && right == -1) {
            this.remove(node);
            return -1;
        }
        else if (left != -1 && right != -1) {
            this.counts[node] = 0.0D;
            return node;
        }
        else {
            this.remove(node);
            return left != -1 ? left : right;
        }
    }

    private void remove(int node)
    {
        if (node == this.nextNode - 1) {
            --this.nextNode;
        }
        else {
            this.pushFree(node);
        }

        if (node == this.root) {
            this.root = -1;
        }
    }

    private void pushFree(int node)
    {
        this.lefts[node] = this.firstFree;
        this.firstFree = node;
        ++this.freeCount;
    }

    private int popFree()
    {
        int node = this.firstFree;
        if (node == -1) {
            return node;
        }
        else {
            this.firstFree = this.lefts[this.firstFree];
            --this.freeCount;
            return node;
        }
    }

    private void postOrderTraversal(int node, QuantileDigest.Callback callback)
    {
        this.postOrderTraversal(node, callback, QuantileDigest.TraversalOrder.FORWARD);
    }

    private void postOrderTraversal(int node, QuantileDigest.Callback callback, QuantileDigest.TraversalOrder order)
    {
        if (order == QuantileDigest.TraversalOrder.FORWARD) {
            this.postOrderTraversal(node, callback, this.lefts, this.rights);
        }
        else {
            this.postOrderTraversal(node, callback, this.rights, this.lefts);
        }
    }

    private boolean postOrderTraversal(int node, QuantileDigest.Callback callback, int[] lefts, int[] rights)
    {
        if (node == -1) {
            return false;
        }
        else {
            int first = lefts[node];
            int second = rights[node];
            if (first != -1 && !this.postOrderTraversal(first, callback, lefts, rights)) {
                return false;
            }
            else {
                return second != -1 && !this.postOrderTraversal(second, callback, lefts, rights) ? false : callback.process(node);
            }
        }
    }

    public double getConfidenceFactor()
    {
        return this.computeMaxPathWeight(this.root) * 1.0D / this.weightedCount;
    }

    @VisibleForTesting
    boolean equivalent(QuantileDigest other)
    {
        return this.getNodeCount() == other.getNodeCount() && this.min == other.min && this.max == other.max && this.weightedCount == other.weightedCount && this.alpha == other.alpha;
    }

    private void rescaleToCommonLandmark(QuantileDigest one, QuantileDigest two)
    {
        long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(this.ticker.read());
        long targetLandmark = Math.max(one.landmarkInSeconds, two.landmarkInSeconds);
        if (nowInSeconds - targetLandmark >= 50L) {
            targetLandmark = nowInSeconds;
        }

        if (targetLandmark != one.landmarkInSeconds) {
            one.rescale(targetLandmark);
        }

        if (targetLandmark != two.landmarkInSeconds) {
            two.rescale(targetLandmark);
        }
    }

    private double computeMaxPathWeight(int node)
    {
        if (node != -1 && this.levels[node] != 0) {
            double leftMaxWeight = this.computeMaxPathWeight(this.lefts[node]);
            double rightMaxWeight = this.computeMaxPathWeight(this.rights[node]);
            return Math.max(leftMaxWeight, rightMaxWeight) + this.counts[node];
        }
        else {
            return 0.0D;
        }
    }

    @VisibleForTesting
    void validate()
    {
        AtomicDouble sum = new AtomicDouble();
        AtomicInteger nodeCount = new AtomicInteger();
        Set<Integer> freeSlots = this.computeFreeList();
        Preconditions.checkState(freeSlots.size() == this.freeCount, "Free count (%s) doesn't match actual free slots: %s", this.freeCount, freeSlots.size());
        if (this.root != -1) {
            this.validateStructure(this.root, freeSlots);
            this.postOrderTraversal(this.root, (node) -> {
                sum.addAndGet(this.counts[node]);
                nodeCount.incrementAndGet();
                return true;
            });
        }

        Preconditions.checkState(Math.abs(sum.get() - this.weightedCount) < 1.0E-5D, "Computed weight (%s) doesn't match summary (%s)", sum.get(), this.weightedCount);
        Preconditions.checkState(nodeCount.get() == this.getNodeCount(), "Actual node count (%s) doesn't match summary (%s)", nodeCount.get(), this.getNodeCount());
    }

    private void validateStructure(int node, Set<Integer> freeNodes)
    {
        Preconditions.checkState(this.levels[node] >= 0);
        Preconditions.checkState(!freeNodes.contains(node), "Node is in list of free slots: %s", node);
        if (this.lefts[node] != -1) {
            this.validateBranchStructure(node, this.lefts[node], this.rights[node], true);
            this.validateStructure(this.lefts[node], freeNodes);
        }

        if (this.rights[node] != -1) {
            this.validateBranchStructure(node, this.rights[node], this.lefts[node], false);
            this.validateStructure(this.rights[node], freeNodes);
        }
    }

    private void validateBranchStructure(int parent, int child, int otherChild, boolean isLeft)
    {
        Preconditions.checkState(this.levels[child] < this.levels[parent], "Child level (%s) should be smaller than parent level (%s)", this.levels[child], this.levels[parent]);
        long branch = this.values[child] & 1L << this.levels[parent] - 1;
        Preconditions.checkState(branch == 0L && isLeft || branch != 0L && !isLeft, "Value of child node is inconsistent with its branch");
        Preconditions.checkState(this.counts[parent] > 0.0D || this.counts[child] > 0.0D || otherChild != -1, "Found a linear chain of zero-weight nodes");
    }

    private Set<Integer> computeFreeList()
    {
        Set<Integer> freeSlots = new HashSet();

        for (int index = this.firstFree; index != -1; index = this.lefts[index]) {
            freeSlots.add(index);
        }

        return freeSlots;
    }

    public String toGraphviz()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("digraph QuantileDigest {\n").append("\tgraph [ordering=\"out\"];");
        List<Integer> nodes = new ArrayList();
        this.postOrderTraversal(this.root, (nodex) -> {
            nodes.add(nodex);
            return true;
        });
        Multimap<Byte, Integer> nodesByLevel = Multimaps.index(nodes, (input) -> {
            return this.levels[input];
        });
        Iterator var4 = nodesByLevel.asMap().entrySet().iterator();

        while (var4.hasNext()) {
            Entry<Byte, Collection<Integer>> entry = (Entry) var4.next();
            builder.append("\tsubgraph level_" + entry.getKey() + " {\n").append("\t\trank = same;\n");
            Iterator var6 = ((Collection) entry.getValue()).iterator();

            while (var6.hasNext()) {
                int node = (Integer) var6.next();
                if (this.levels[node] == 0) {
                    builder.append(String.format("\t\t%s [label=\"%s:[%s]@%s\\n%s\", shape=rect, style=filled,color=%s];\n", idFor(node), node, this.lowerBound(node), this.levels[node], this.counts[node], this.counts[node] > 0.0D ? "salmon2" : "white"));
                }
                else {
                    builder.append(String.format("\t\t%s [label=\"%s:[%s..%s]@%s\\n%s\", shape=rect, style=filled,color=%s];\n", idFor(node), node, this.lowerBound(node), this.upperBound(node), this.levels[node], this.counts[node], this.counts[node] > 0.0D ? "salmon2" : "white"));
                }
            }

            builder.append("\t}\n");
        }

        var4 = nodes.iterator();

        while (var4.hasNext()) {
            int node = (Integer) var4.next();
            if (this.lefts[node] != -1) {
                builder.append(String.format("\t%s -> %s [style=\"%s\"];\n", idFor(node), idFor(this.lefts[node]), this.levels[node] - this.levels[this.lefts[node]] == 1 ? "solid" : "dotted"));
            }

            if (this.rights[node] != -1) {
                builder.append(String.format("\t%s -> %s [style=\"%s\"];\n", idFor(node), idFor(this.rights[node]), this.levels[node] - this.levels[this.rights[node]] == 1 ? "solid" : "dotted"));
            }
        }

        builder.append("}\n");
        return builder.toString();
    }

    private static String idFor(int node)
    {
        return String.format("node_%x", node);
    }

    private static long longToBits(long value)
    {
        return value ^ -9223372036854775808L;
    }

    private static long bitsToLong(long bits)
    {
        return bits ^ -9223372036854775808L;
    }

    private long getBranchMask(byte level)
    {
        return 1L << level - 1;
    }

    private long upperBound(int node)
    {
        long mask = 0L;
        if (this.levels[node] > 0) {
            mask = -1L >>> 64 - this.levels[node];
        }

        return bitsToLong(this.values[node] | mask);
    }

    private long lowerBound(int node)
    {
        long mask = 0L;
        if (this.levels[node] > 0) {
            mask = -1L >>> 64 - this.levels[node];
        }

        return bitsToLong(this.values[node] & ~mask);
    }

    private long middle(int node)
    {
        long lower = this.lowerBound(node);
        long upper = this.upperBound(node);
        return lower + (upper - lower) / 2L;
    }

    private static Ticker noOpTicker()
    {
        return new Ticker()
        {
            public long read()
            {
                return 0L;
            }
        };
    }

    public interface MiddleFunction
    {
        QuantileDigest.MiddleFunction DEFAULT = (lowerBound, upperBound) -> {
            return (double) lowerBound + (double) (upperBound - lowerBound) / 2.0D;
        };

        double middle(long lowerBound, long upperBound);
    }

    public interface TruncMeanValueFunction
    {
        QuantileDigest.TruncMeanValueFunction DEFAULT = value -> value;

        double value(long value);
    }

    private static class Flags
    {
        public static final int HAS_LEFT = 1;
        public static final int HAS_RIGHT = 2;
        public static final byte FORMAT = 0;

        private Flags()
        {
        }
    }

    private interface Callback
    {
        boolean process(int node);
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
            return this.count;
        }

        public double getMean()
        {
            return this.mean;
        }

        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            else if (o != null && this.getClass() == o.getClass()) {
                QuantileDigest.Bucket bucket = (QuantileDigest.Bucket) o;
                if (Double.compare(bucket.count, this.count) != 0) {
                    return false;
                }
                else {
                    return Double.compare(bucket.mean, this.mean) == 0;
                }
            }
            else {
                return false;
            }
        }

        public int hashCode()
        {
            long temp = this.count != 0.0D ? Double.doubleToLongBits(this.count) : 0L;
            int result = (int) (temp ^ temp >>> 32);
            temp = this.mean != 0.0D ? Double.doubleToLongBits(this.mean) : 0L;
            result = 31 * result + (int) (temp ^ temp >>> 32);
            return result;
        }

        public String toString()
        {
            return String.format("[count: %f, mean: %f]", this.count, this.mean);
        }
    }

    private static final class HistogramBuilderStateHolder
    {
        double sum;
        double lastSum;
        double bucketWeightedSum;

        private HistogramBuilderStateHolder()
        {
        }
    }

    private static enum TraversalOrder
    {
        FORWARD,
        REVERSE;

        private TraversalOrder()
        {
        }
    }
}
