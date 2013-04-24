package com.facebook.presto.hive.util;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Ordering;
import com.google.common.collect.PeekingIterator;
import com.google.common.util.concurrent.AtomicDouble;

import javax.annotation.concurrent.ThreadSafe;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

/**
 * Copied with minor adjustments from io.airlift.stats.Distribution
 * TODO: remove this temporary class when we upgrade of airlift 0.76
 */
@ThreadSafe
public class QuantileDigest2
{
    private static final int MAX_BITS = 64;
    private static final double MAX_SIZE_FACTOR = 1.5;

    // needs to be such that Math.exp(alpha * seconds) does not grow too big
    static final long RESCALE_THRESHOLD_SECONDS = 50;
    static final double ZERO_WEIGHT_THRESHOLD = 1e-5;

    private final double maxError;
    private final Ticker ticker;
    private final double alpha;
    private final boolean compressAutomatically;

    private Node root;

    private double weightedCount;
    private long max;
    private long min = Long.MAX_VALUE;

    private long landmarkInSeconds;

    private int totalNodeCount = 0;
    private int nonZeroNodeCount = 0;
    private int compressions = 0;
    private int maxTotalNodeCount = 0;
    private int maxTotalNodesAfterCompress = 0;

    private enum TraversalOrder
    {
        FORWARD, REVERSE
    }

    /**
     * <p>Create a QuantileDigest with a maximum error guarantee of "maxError" and no decay.
     *
     * @param maxError the max error tolerance
     */
    public QuantileDigest2(double maxError)
    {
        this(maxError, 0);
    }

    /**
     *<p>Create a QuantileDigest with a maximum error guarantee of "maxError" and exponential decay
     * with factor "alpha".</p>
     *
     * @param maxError the max error tolerance
     * @param alpha the exponential decay factor
     */
    public QuantileDigest2(double maxError, double alpha)
    {
        this(maxError, alpha, Ticker.systemTicker(), true);
    }

    @VisibleForTesting
    QuantileDigest2(double maxError, double alpha, Ticker ticker, boolean compressAutomatically)
    {
        checkArgument(maxError >= 0 && maxError <= 1, "maxError must be in range [0, 1]");
        checkArgument(alpha >= 0 && alpha < 1, "alpha must be in range [0, 1)");

        this.maxError = maxError;
        this.alpha = alpha;
        this.ticker = ticker;
        this.compressAutomatically = compressAutomatically;

        landmarkInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());
    }

    /**
     * Adds a value to this digest. The value must be >= 0
     */
    public synchronized void add(long value, long valueCount)
    {
        checkArgument(value >= 0, "value must be >= 0");

        long nowInSeconds = TimeUnit.NANOSECONDS.toSeconds(ticker.read());

        int maxExpectedNodeCount = 3 * calculateCompressionFactor();
        if (nowInSeconds - landmarkInSeconds >= RESCALE_THRESHOLD_SECONDS) {
            rescale(nowInSeconds);
            compress(); // need to compress to get rid of nodes that may have decayed to ~ 0
        }
        else if (nonZeroNodeCount > MAX_SIZE_FACTOR * maxExpectedNodeCount && compressAutomatically) {
            // The size (number of non-zero nodes) of the digest is at most 3 * compression factor
            // If we're over MAX_SIZE_FACTOR of the expected size, compress
            // Note: we don't compress as soon as we go over expectedNodeCount to avoid unnecessarily
            // running a compression for every new added element when we're close to boundary
            compress();
        }

        double weight = weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read())) * valueCount;
        weightedCount += weight;

        max = Math.max(max, value);
        min = Math.min(min, value);
        insert(value, weight);
    }

    public synchronized void add(long value)
    {
        add(value, 1);
    }

    /**
     * Gets the values at the specified quantiles +/- maxError. The list of quantiles must be sorted
     * in increasing order, and each value must be in the range [0, 1]
     */
    public synchronized List<Long> getQuantiles(List<Double> quantiles)
    {
        checkArgument(Ordering.natural().isOrdered(quantiles), "quantiles must be sorted in increasing order");
        for (double quantile : quantiles) {
            checkArgument(quantile >= 0 && quantile <= 1, "quantile must be between [0,1]");
        }

        final ImmutableList.Builder<Long> builder = ImmutableList.builder();
        final PeekingIterator<Double> iterator = Iterators.peekingIterator(quantiles.iterator());

        postOrderTraversal(root, new Callback()
        {
            private double sum = 0;

            public boolean process(Node node)
            {
                sum += node.weightedCount;

                while (iterator.hasNext() && sum > iterator.peek() * weightedCount) {
                    iterator.next();

                    // we know the max value ever seen, so cap the percentile to provide better error
                    // bounds in this case
                    long value = Math.min(node.getUpperBound(), max);

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

    /**
     * Gets the value at the specified quantile +/- maxError. The quantile must be in the range [0, 1]
     */
    public synchronized long getQuantile(double quantile)
    {
        return getQuantiles(ImmutableList.of(quantile)).get(0);
    }

    /**
     * Number (decayed) of elements added to this quantile digest
     */
    public synchronized double getCount()
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
    public synchronized List<Bucket> getHistogram(List<Long> bucketUpperBounds)
    {
        checkArgument(
                Ordering.natural().isOrdered(bucketUpperBounds),
                "buckets must be sorted in increasing order"
        );

        final ImmutableList.Builder<Bucket> builder = ImmutableList.builder();
        final PeekingIterator<Long> iterator = Iterators.peekingIterator(bucketUpperBounds.iterator());

        final AtomicDouble sum = new AtomicDouble();
        final AtomicDouble lastSum = new AtomicDouble();

        // for computing weighed average of values in bucket
        final AtomicDouble bucketWeightedSum = new AtomicDouble();

        final double normalizationFactor = weight(TimeUnit.NANOSECONDS.toSeconds(ticker.read()));

        postOrderTraversal(root, new Callback()
        {
            public boolean process(Node node)
            {

                while (iterator.hasNext() && iterator.peek() <= node.getUpperBound()) {
                    double bucketCount = sum.get() - lastSum.get();

                    Bucket bucket = new Bucket(
                            bucketCount / normalizationFactor, bucketWeightedSum.get() / bucketCount);

                    builder.add(bucket);
                    lastSum.set(sum.get());
                    bucketWeightedSum.set(0);
                    iterator.next();
                }

                bucketWeightedSum.addAndGet(node.getMiddle() * node.weightedCount);
                sum.addAndGet(node.weightedCount);
                return iterator.hasNext();
            }
        });

        while (iterator.hasNext()) {
            double bucketCount = sum.get() - lastSum.get();
            Bucket bucket = new Bucket(
                    bucketCount / normalizationFactor, bucketWeightedSum.get() / bucketCount);

            builder.add(bucket);

            iterator.next();
        }

        return builder.build();
    }

    public long getMin()
    {
        final AtomicLong chosen = new AtomicLong(min);
        postOrderTraversal(root, new Callback()
        {
            public boolean process(Node node)
            {
                if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
                    chosen.set(node.getLowerBound());
                    return false;
                }
                return true;
            }
        }, TraversalOrder.FORWARD);

        return Math.max(min, chosen.get());
    }

    public long getMax()
    {
        final AtomicLong chosen = new AtomicLong(max);
        postOrderTraversal(root, new Callback()
        {
            public boolean process(Node node)
            {
                if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
                    chosen.set(node.getUpperBound());
                    return false;
                }
                return true;
            }
        }, TraversalOrder.REVERSE);

        return Math.min(max, chosen.get());
    }

    @VisibleForTesting
    synchronized int getTotalNodeCount()
    {
        return totalNodeCount;
    }

    @VisibleForTesting
    synchronized int getNonZeroNodeCount()
    {
        return nonZeroNodeCount;
    }

    @VisibleForTesting
    synchronized int getCompressions()
    {
        return compressions;
    }

    @VisibleForTesting
    synchronized void compress()
    {
        ++compressions;

        final int compressionFactor = calculateCompressionFactor();

        postOrderTraversal(root, new Callback()
        {
            public boolean process(Node node)
            {
                if (node.isLeaf()) {
                    return true;
                }

                // if children's weights are ~0 remove them and shift the weight to their parent

                double leftWeight = 0;
                if (node.left != null) {
                    leftWeight = node.left.weightedCount;
                }

                double rightWeight = 0;
                if (node.right != null) {
                    rightWeight = node.right.weightedCount;
                }

                boolean shouldCompress = node.weightedCount + leftWeight + rightWeight <
                        weightedCount / compressionFactor;

                double oldNodeWeight = node.weightedCount;
                if (shouldCompress || leftWeight < ZERO_WEIGHT_THRESHOLD) {
                    node.left = tryRemove(node.left);

                    weightedCount += leftWeight;
                    node.weightedCount += leftWeight;
                }

                if (shouldCompress || rightWeight < ZERO_WEIGHT_THRESHOLD) {
                    node.right = tryRemove(node.right);

                    weightedCount += rightWeight;
                    node.weightedCount += rightWeight;
                }

                if (oldNodeWeight < ZERO_WEIGHT_THRESHOLD && node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
                    ++nonZeroNodeCount;
                }

                return true;
            }
        });

        if (root != null && root.weightedCount < ZERO_WEIGHT_THRESHOLD) {
            root = tryRemove(root);
        }

        maxTotalNodesAfterCompress = Math.max(maxTotalNodesAfterCompress, totalNodeCount);
    }

    private double weight(long timestamp)
    {
        return Math.exp(alpha * (timestamp - landmarkInSeconds));
    }

    private void rescale(long newLandmarkInSeconds)
    {
        // rescale the weights based on a new landmark to avoid numerical overflow issues

        final double factor = Math.exp(-alpha * (newLandmarkInSeconds - landmarkInSeconds));

        weightedCount *= factor;

        postOrderTraversal(root, new Callback()
        {
            public boolean process(Node node)
            {
                double oldWeight = node.weightedCount;

                node.weightedCount *= factor;

                if (oldWeight >= ZERO_WEIGHT_THRESHOLD && node.weightedCount < ZERO_WEIGHT_THRESHOLD) {
                    --nonZeroNodeCount;
                }

                return true;
            }
        });

        landmarkInSeconds = newLandmarkInSeconds;
    }

    private int calculateCompressionFactor()
    {
        if (root == null) {
            return 1;
        }

        return Math.max((int) ((root.level + 1) / maxError), 1);
    }

    private void insert(long value, double weight)
    {
        long lastBranch = 0;
        Node parent = null;
        Node current = root;

        while (true) {
            if (current == null) {
                setChild(parent, lastBranch, createLeaf(value, weight));
                return;
            }
            else if ((value >>> current.level) != (current.value >>> current.level)) {
                // if value and node.value are not in the same branch given node's level,
                // insert a parent above them at the point at which branches diverge
                setChild(parent, lastBranch, makeSiblings(current, createLeaf(value, weight)));
                return;
            }
            else if (current.level == 0 && current.value == value) {
                // found the node

                double oldWeight = current.weightedCount;

                current.weightedCount += weight;

                if (current.weightedCount >= ZERO_WEIGHT_THRESHOLD && oldWeight < ZERO_WEIGHT_THRESHOLD) {
                    ++nonZeroNodeCount;
                }

                return;
            }

            // we're on the correct branch of the tree and we haven't reached a leaf, so keep going down
            long branch = value & current.getBranchMask();

            parent = current;
            lastBranch = branch;

            if (branch == 0) {
                current = current.left;
            }
            else {
                current = current.right;
            }
        }
    }

    private void setChild(Node parent, long branch, Node child)
    {
        if (parent == null) {
            root = child;
        }
        else if (branch == 0) {
            parent.left = child;
        }
        else {
            parent.right = child;
        }
    }

    private Node makeSiblings(Node node, Node sibling)
    {
        int parentLevel = MAX_BITS - Long.numberOfLeadingZeros(node.value ^ sibling.value);

        Node parent = new Node(node.value, parentLevel, 0);

        // the branch is given by the bit at the level one below parent
        long branch = sibling.value & parent.getBranchMask();
        if (branch == 0) {
            parent.left = sibling;
            parent.right = node;
        }
        else {
            parent.left = node;
            parent.right = sibling;
        }

        ++totalNodeCount;
        maxTotalNodeCount = Math.max(maxTotalNodeCount, totalNodeCount);

        return parent;
    }

    private Node createLeaf(long value, double weight)
    {
        ++totalNodeCount;
        maxTotalNodeCount = Math.max(maxTotalNodeCount, totalNodeCount);
        ++nonZeroNodeCount;
        return new Node(value, 0, weight);
    }

    /**
     * Remove the node if possible or set its count to 0 if it has children and
     * it needs to be kept around
     */
    private Node tryRemove(Node node)
    {
        if (node == null) {
            return null;
        }

        if (node.weightedCount >= ZERO_WEIGHT_THRESHOLD) {
            --nonZeroNodeCount;
        }

        weightedCount -= node.weightedCount;

        Node result = null;
        if (node.isLeaf()) {
            --totalNodeCount;
        }
        else if (node.hasSingleChild()) {
            result = node.getSingleChild();
            --totalNodeCount;
        }
        else {
            node.weightedCount = 0;
            result = node;
        }

        return result;
    }

    private boolean postOrderTraversal(Node node, Callback callback)
    {
        return postOrderTraversal(node, callback, TraversalOrder.FORWARD);
    }

    // returns true if traversal should continue
    private boolean postOrderTraversal(Node node, Callback callback, TraversalOrder order)
    {
        if (node == null) {
            return false;
        }

        Node first;
        Node second;

        if (order == TraversalOrder.FORWARD) {
            first = node.left;
            second = node.right;
        }
        else {
            first = node.right;
            second = node.left;
        }

        if (first != null && !postOrderTraversal(first, callback, order)) {
            return false;
        }

        if (second != null && !postOrderTraversal(second, callback, order)) {
            return false;
        }

        return callback.process(node);
    }

    /**
     * Computes the maximum error of the current digest
     */
    public synchronized double getConfidenceFactor()
    {
        return computeMaxPathWeight(root) * 1.0 / weightedCount;
    }

    /**
     * Computes the max "weight" of any path starting at node and ending at a leaf in the
     * hypothetical complete tree. The weight is the sum of counts in the ancestors of a given node
     */
    private double computeMaxPathWeight(Node node)
    {
        if (node == null || node.level == 0) {
            return 0;
        }

        double leftMaxWeight = computeMaxPathWeight(node.left);
        double rightMaxWeight = computeMaxPathWeight(node.right);

        return Math.max(leftMaxWeight, rightMaxWeight) + node.weightedCount;
    }

    @VisibleForTesting
    synchronized void validate()
    {
        final AtomicDouble sumOfWeights = new AtomicDouble();
        final AtomicInteger actualNodeCount = new AtomicInteger();
        final AtomicInteger actualNonZeroNodeCount = new AtomicInteger();

        if (root != null) {
            validateStructure(root);

            postOrderTraversal(root, new Callback()
            {
                @Override
                public boolean process(Node node)
                {
                    sumOfWeights.addAndGet(node.weightedCount);
                    actualNodeCount.incrementAndGet();

                    if (node.weightedCount > ZERO_WEIGHT_THRESHOLD) {
                        actualNonZeroNodeCount.incrementAndGet();
                    }

                    return true;
                }
            });
        }

        checkState(Math.abs(sumOfWeights.get() - weightedCount) < ZERO_WEIGHT_THRESHOLD,
                "Computed weight (%s) doesn't match summary (%s)", sumOfWeights.get(),
                weightedCount);

        checkState(actualNodeCount.get() == totalNodeCount,
                "Actual node count (%s) doesn't match summary (%s)",
                actualNodeCount.get(), totalNodeCount);

        checkState(actualNonZeroNodeCount.get() == nonZeroNodeCount,
                "Actual non-zero node count (%s) doesn't match summary (%s)",
                actualNonZeroNodeCount.get(), nonZeroNodeCount);
    }

    private void validateStructure(Node node)
    {
        checkState(node.level >= 0);

        if (node.left != null) {
            validateBranchStructure(node, node.left, node.right, true);
            validateStructure(node.left);
        }

        if (node.right != null) {
            validateBranchStructure(node, node.right, node.left, false);
            validateStructure(node.right);
        }
    }

    private void validateBranchStructure(Node parent, Node child, Node otherChild, boolean isLeft)
    {
        checkState(child.level < parent.level,
                "Child level (%s) should be smaller than parent level (%s)", child.level,
                parent.level);

        long branch = child.value & (1L << (parent.level - 1));
        checkState(branch == 0 && isLeft || branch != 0 && !isLeft,
                "Value of child node is inconsistent with its branch");

        Preconditions.checkState(parent.weightedCount >= ZERO_WEIGHT_THRESHOLD ||
                child.weightedCount >= ZERO_WEIGHT_THRESHOLD || otherChild != null,
                "Found a linear chain of zero-weight nodes");
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

    private static class Node
    {
        private double weightedCount;
        private int level;
        private long value;
        private Node left;
        private Node right;

        private Node(long value, int level, double weightedCount)
        {
            this.value = value;
            this.level = level;
            this.weightedCount = weightedCount;
        }

        public boolean isLeaf()
        {
            return left == null && right == null;
        }

        public boolean hasSingleChild()
        {
            return left == null && right != null || left != null && right == null;
        }

        public Node getSingleChild()
        {
            checkState(hasSingleChild(), "Node does not have a single child");
            return Objects.firstNonNull(left, right);
        }

        public long getUpperBound()
        {
            // set all lsb below level to 1 (we're looking for the highest value of the range covered
            // by this node)
            long mask = (1L << level) - 1;
            return value | mask;
        }

        public long getBranchMask()
        {
            return (1L << (level - 1));
        }

        public long getLowerBound()
        {
            // set all lsb below level to 0 (we're looking for the lowes value of the range covered
            // by this node)
            long mask = (0x7FFFFFFFFFFFFFFFL << level);
            return value & mask;
        }

        public long getMiddle()
        {
            return getLowerBound() + (getUpperBound() - getLowerBound()) / 2;
        }

        public String toString()
        {
            return format("%s (level = %d, count = %s, left = %s, right = %s)", value, level,
                    weightedCount, left != null, right != null);
        }
    }

    private static interface Callback
    {
        /**
         * @param node the node to process
         * @return true if processing should continue
         */
        boolean process(Node node);
    }
}
