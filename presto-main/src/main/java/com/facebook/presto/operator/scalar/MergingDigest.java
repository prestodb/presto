/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.operator.scalar;

import com.google.common.base.Preconditions;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Maintains a t-digest by collecting new points in a buffer that is then sorted occasionally and merged
 * into a sorted array that contains previously computed centroids.
 * <p>
 * This can be very fast because the cost of sorting and merging is amortized over several insertion. If
 * we keep N centroids total and have the input array is k long, then the amortized cost is something like
 * <p>
 * N/k + log k
 * <p>
 * These costs even out when N/k = log k.  Balancing costs is often a good place to start in optimizing an
 * algorithm.  For different values of compression factor, the following table shows estimated asymptotic
 * values of N and suggested values of k:
 * <table>
 * <thead>
 * <tr><td>Compression</td><td>N</td><td>k</td></tr>
 * </thead>
 * <tbody>
 * <tr><td>50</td><td>78</td><td>25</td></tr>
 * <tr><td>100</td><td>157</td><td>42</td></tr>
 * <tr><td>200</td><td>314</td><td>73</td></tr>
 * </tbody>
 * <caption>Sizing considerations for t-digest</caption>
 * </table>
 * <p>
 * The virtues of this kind of t-digest implementation include:
 * <ul>
 * <li>No allocation is required after initialization</li>
 * <li>The data structure automatically compresses existing centroids when possible</li>
 * <li>No Java object overhead is incurred for centroids since data is kept in primitive arrays</li>
 * </ul>
 * <p>
 * The current implementation takes the liberty of using ping-pong buffers for implementing the merge resulting
 * in a substantial memory penalty, but the complexity of an in place merge was not considered as worthwhile
 * since even with the overhead, the memory cost is less than 40 bytes per centroid which is much less than half
 * what the AVLTreeDigest uses and no dynamic allocation is required at all.
 */

public class MergingDigest
        extends TDigest
{
    private int mergeCount;

    private final double publicCompression;
    private final double compression;

    // points to the first unused centroid
    private int activeCentroids;

    // sum_i weight[i]  See also unmergedWeight
    private double totalWeight;

    // number of points that have been added to each merged centroid
    private final double[] weight;
    // mean of points added to each merged centroid
    private final double[] mean;

    // history of all data added to centroids (for testing purposes)
    private List<List<Double>> data;

    // sum_i tempWeight[i]
    private double unmergedWeight;

    // this is the index of the next temporary centroid
    // this is a more Java-like convention than activeCentroids uses
    private int tempUsed;
    private final double[] tempWeight;
    private final double[] tempMean;
    private List<List<Double>> tempData;

    // array used for sorting the temp centroids.  This is a field
    // to avoid allocations during operation
    private final int[] order;

    // if true, alternate upward and downward merge passes
    public boolean useAlternatingSort = true;
    // if true, use higher working value of compression during construction, then reduce on presentation
    public boolean useTwoLevelCompression = true;

    // this forces centroid merging based on size limit rather than
    // based on accumulated k-index. This can be much faster since we
    // scale functions are more expensive than the corresponding
    // weight limits.
    public static boolean useWeightLimit = true;

    /**
     * Allocates a buffer merging t-digest.  This is the normally used constructor that
     * allocates default sized internal arrays.  Other versions are available, but should
     * only be used for special cases.
     *
     * @param compression The compression factor
     */
    @SuppressWarnings("WeakerAccess")
    public MergingDigest(double compression)
    {
        this(compression, -1);
    }

    /**
     * If you know the size of the temporary buffer for incoming points, you can use this entry point.
     *
     * @param compression Compression factor for t-digest.  Same as 1/\delta in the paper.
     * @param bufferSize How many samples to retain before merging.
     */
    @SuppressWarnings("WeakerAccess")
    public MergingDigest(double compression, int bufferSize)
    {
        // we can guarantee that we only need ceiling(compression).
        this(compression, bufferSize, -1);
    }

    /**
     * Fully specified constructor.  Normally only used for deserializing a buffer t-digest.
     *
     * @param compression Compression factor
     * @param bufferSize Number of temporary centroids
     * @param size Size of main buffer
     */
    @SuppressWarnings("WeakerAccess")
    public MergingDigest(double compression, int bufferSize, int size)
    {
        // ensure compression >= 10
        // default size = 2 * ceil(compression)
        // default bufferSize = 5 * size
        // scale = max(2, bufferSize / size - 1)
        // compression, publicCompression = sqrt(scale-1)*compression, compression
        // ensure size > 2 * compression + weightLimitFudge
        // ensure bufferSize > 2*size

        // force reasonable value. Anything less than 10 doesn't make much sense because
        // too few centroids are retained
        if (compression < 10) {
            compression = 10;
        }

        // the weight limit is too conservative about sizes and can require a bit of extra room
        double sizeFudge = 0;
        if (useWeightLimit) {
            sizeFudge = 10;
            if (compression < 30) {
                sizeFudge += 20;
            }
        }

        // default size
        size = (int) Math.max(2 * compression + sizeFudge, size);

        // default buffer
        if (bufferSize == -1) {
            // having a big buffer is good for speed
            // experiments show bufferSize = 1 gives half the performance of bufferSize=10
            // bufferSize = 2 gives 40% worse performance than 10
            // but bufferSize = 5 only costs about 5-10%
            //
            //   compression factor     time(us)
            //    50          1         0.275799
            //    50          2         0.151368
            //    50          5         0.108856
            //    50         10         0.102530
            //   100          1         0.215121
            //   100          2         0.142743
            //   100          5         0.112278
            //   100         10         0.107753
            //   200          1         0.210972
            //   200          2         0.148613
            //   200          5         0.118220
            //   200         10         0.112970
            //   500          1         0.219469
            //   500          2         0.158364
            //   500          5         0.127552
            //   500         10         0.121505
            bufferSize = 5 * size;
        }

        // ensure enough space in buffer
        if (bufferSize <= 2 * size) {
            bufferSize = 2 * size;
        }

        // scale is the ratio of extra buffer to the final size
        // we have to account for the fact that we copy all live centroids into the incoming space
        double scale = Math.max(1, bufferSize / size - 1);
        if (!useTwoLevelCompression) {
            scale = 1;
        }

        // publicCompression is how many centroids the user asked for
        // compression is how many we actually keep
        this.publicCompression = compression;
        this.compression = Math.sqrt(scale) * publicCompression;

        // changing the compression could cause buffers to be too small, readjust if so
        if (size < this.compression + sizeFudge) {
            size = (int) Math.ceil(this.compression + sizeFudge);
        }

        // ensure enough space in buffer (possibly again)
        if (bufferSize <= 2 * size) {
            bufferSize = 2 * size;
        }

        weight = new double[size];
        mean = new double[size];

        tempWeight = new double[bufferSize];
        tempMean = new double[bufferSize];
        order = new int[bufferSize];

        activeCentroids = 0;
    }

    /**
     * Turns on internal data recording.
     */
    @Override
    public TDigest recordAllData()
    {
        super.recordAllData();
        data = new ArrayList<>();
        tempData = new ArrayList<>();
        return this;
    }

    @Override
    void add(double mean, int weight, Centroid base)
    {
        add(mean, weight, base.data());
    }

    @Override
    public void add(double mean, int weight)
    {
        add(mean, weight, (List<Double>) null);
    }

    private void add(double mean, int weight, List<Double> history)
    {
        if (Double.isNaN(mean)) {
            throw new IllegalArgumentException("Cannot add NaN to t-digest");
        }
        if (tempUsed >= tempWeight.length - activeCentroids - 1) {
            mergeNewValues();
        }
        int where = tempUsed++;
        tempWeight[where] = weight;
        tempMean[where] = mean;
        unmergedWeight += weight;
        if (mean < min) {
            min = mean;
        }
        if (mean > max) {
            max = mean;
        }

        if (data != null) {
            if (tempData == null) {
                tempData = new ArrayList<>();
            }
            while (tempData.size() <= where) {
                tempData.add(new ArrayList<Double>());
            }
            if (history == null) {
                history = Collections.singletonList(mean);
            }
            tempData.get(where).addAll(history);
        }
    }

    private void add(double[] mean, double[] weight, int count, List<List<Double>> data)
    {
        if (mean.length != weight.length) {
            throw new IllegalArgumentException("Arrays not same length");
        }
        if (mean.length < count + activeCentroids) {
            // make room to add existing centroids
            double[] mean1 = new double[count + activeCentroids];
            System.arraycopy(mean, 0, mean1, 0, count);
            mean = mean1;
            double[] weight1 = new double[count + activeCentroids];
            System.arraycopy(weight, 0, weight1, 0, count);
            weight = weight1;
        }
        double total = 0;
        for (int i = 0; i < count; i++) {
            total += weight[i];
        }
        merge(mean, weight, count, data, null, total, false, compression);
    }

    @Override
    public void add(List<? extends TDigest> others)
    {
        if (others.size() == 0) {
            return;
        }
        int size = activeCentroids;
        for (TDigest other : others) {
            // other.compress();
            size += other.centroidCount();
        }

        double[] mean = new double[size];
        double[] weight = new double[size];
        List<List<Double>> data;
        if (recordAllData) {
            data = new ArrayList<>();
        }
        else {
            data = null;
        }
        int offset = 0;
        for (TDigest other : others) {
            if (other instanceof MergingDigest) {
                MergingDigest digest = (MergingDigest) other;
                System.arraycopy(digest.mean, 0, mean, offset, digest.activeCentroids);
                System.arraycopy(digest.weight, 0, weight, offset, digest.activeCentroids);
                if (data != null) {
                    for (Centroid centroid : other.centroids()) {
                        data.add(centroid.data());
                    }
                }
                offset += digest.activeCentroids;
            }
            else {
                for (Centroid centroid : other.centroids()) {
                    mean[offset] = centroid.mean();
                    weight[offset] = centroid.count();
                    if (recordAllData) {
                        Preconditions.checkArgument(data != null, "data is null");
                        data.add(centroid.data());
                    }
                    offset++;
                }
            }
        }
        add(mean, weight, size, data);
    }

    private void mergeNewValues()
    {
        mergeNewValues(false, compression);
    }

    private void mergeNewValues(boolean force, double compression)
    {
        if (totalWeight == 0 && unmergedWeight == 0) {
            // seriously nothing to do
            return;
        }
        if (force || unmergedWeight > 0) {
            // note that we run the merge in reverse every other merge to avoid left-to-right bias in merging
            merge(tempMean, tempWeight, tempUsed, tempData, order, unmergedWeight,
                    useAlternatingSort & mergeCount % 2 == 1, compression);
            mergeCount++;
            tempUsed = 0;
            unmergedWeight = 0;
            if (data != null) {
                tempData = new ArrayList<>();
            }
        }
    }

    private void merge(double[] incomingMean, double[] incomingWeight, int incomingCount,
            List<List<Double>> incomingData, int[] incomingOrder,
            double unmergedWeight, boolean runBackwards, double compression)
    {
        System.arraycopy(mean, 0, incomingMean, incomingCount, activeCentroids);
        System.arraycopy(weight, 0, incomingWeight, incomingCount, activeCentroids);
        incomingCount += activeCentroids;

        if (incomingData != null) {
            for (int i = 0; i < activeCentroids; i++) {
                Preconditions.checkArgument(data != null, "data is null");
                incomingData.add(data.get(i));
            }
            data = new ArrayList<>();
        }
        if (incomingOrder == null) {
            incomingOrder = new int[incomingCount];
        }
        Sort.sort(incomingOrder, incomingMean, incomingCount);
        // option to run backwards is to investigate bias in errors
        if (runBackwards) {
            Sort.reverse(incomingOrder, 0, incomingCount);
        }

        totalWeight += unmergedWeight;

        Preconditions.checkArgument((activeCentroids + incomingCount) > 0, "error");
        activeCentroids = 0;
        mean[activeCentroids] = incomingMean[incomingOrder[0]];
        weight[activeCentroids] = incomingWeight[incomingOrder[0]];
        double weightSoFar = 0;
        if (data != null) {
            Preconditions.checkArgument(data != null, "data is null");
            data.add(incomingData.get(incomingOrder[0]));
        }

        // weight will contain all zeros after this loop

        double normalizer = scale.normalizer(compression, totalWeight);
        double k1 = scale.k(0, normalizer);
        double weightLimit = totalWeight * scale.q(k1 + 1, normalizer);
        for (int i = 1; i < incomingCount; i++) {
            int ix = incomingOrder[i];
            double proposedWeight = weight[activeCentroids] + incomingWeight[ix];
            double projectedWeight = weightSoFar + proposedWeight;
            boolean addThis;
            if (useWeightLimit) {
                double q0 = weightSoFar / totalWeight;
                double q2 = (weightSoFar + proposedWeight) / totalWeight;
                addThis = proposedWeight <= totalWeight * Math.min(scale.max(q0, normalizer), scale.max(q2, normalizer));
            }
            else {
                addThis = projectedWeight <= weightLimit;
            }

            if (addThis) {
                // next point will fit
                // so merge into existing centroid
                weight[activeCentroids] += incomingWeight[ix];
                mean[activeCentroids] = mean[activeCentroids] + (incomingMean[ix] - mean[activeCentroids]) * incomingWeight[ix] / weight[activeCentroids];
                incomingWeight[ix] = 0;

                if (data != null) {
                    while (data.size() <= activeCentroids) {
                        data.add(new ArrayList<Double>());
                    }
                    Preconditions.checkArgument(incomingData != null, "data is null");
                    Preconditions.checkArgument(data.get(activeCentroids) != incomingData.get(ix), "error");
                    data.get(activeCentroids).addAll(incomingData.get(ix));
                }
            }
            else {
                // didn't fit ... move to next output, copy out first centroid
                weightSoFar += weight[activeCentroids];
                if (!useWeightLimit) {
                    k1 = scale.k(weightSoFar / totalWeight, normalizer);
                    weightLimit = totalWeight * scale.q(k1 + 1, normalizer);
                }

                activeCentroids++;
                mean[activeCentroids] = incomingMean[ix];
                weight[activeCentroids] = incomingWeight[ix];
                incomingWeight[ix] = 0;

                if (data != null) {
                    Preconditions.checkArgument(data != null, "data is null");
                    Preconditions.checkArgument(data.size() == activeCentroids, "error");
                    data.add(incomingData.get(ix));
                }
            }
        }
        // points to next empty cell
        activeCentroids++;

        // sanity check
        double sum = 0;
        for (int i = 0; i < activeCentroids; i++) {
            sum += weight[i];
        }

        Preconditions.checkArgument(sum == totalWeight, "error");
        if (runBackwards) {
            Sort.reverse(mean, 0, activeCentroids);
            Sort.reverse(weight, 0, activeCentroids);
            if (data != null) {
                Collections.reverse(data);
            }
        }

        if (totalWeight > 0) {
            min = Math.min(min, mean[0]);
            max = Math.max(max, mean[activeCentroids - 1]);
        }
    }

    /**
     * Exposed for testing.
     */
    int checkWeights()
    {
        return checkWeights(weight, totalWeight, activeCentroids);
    }

    private int checkWeights(double[] weight, double total, int last)
    {
        int badCount = 0;

        int n = last;
        if (weight[n] > 0) {
            n++;
        }

        double normalizer = scale.normalizer(publicCompression, totalWeight);
        double k1 = scale.k(0, normalizer);
        double q = 0;
        double left = 0;
        String header = "\n";
        for (int i = 0; i < n; i++) {
            double dq = weight[i] / total;
            double k2 = scale.k(q + dq, normalizer);
            q += dq / 2;
            if (k2 - k1 > 1 && weight[i] != 1) {
                System.out.printf("%sOversize centroid at " +
                                "%d, k0=%.2f, k1=%.2f, dk=%.2f, w=%.2f, q=%.4f, dq=%.4f, left=%.1f, current=%.2f maxw=%.2f\n",
                        header, i, k1, k2, k2 - k1, weight[i], q, dq, left, weight[i], totalWeight * scale.max(q, normalizer));
                header = "";
                badCount++;
            }
            if (k2 - k1 > 4 && weight[i] != 1) {
                throw new IllegalStateException(
                        String.format("Egregiously oversized centroid at " +
                                        "%d, k0=%.2f, k1=%.2f, dk=%.2f, w=%.2f, q=%.4f, dq=%.4f, left=%.1f, current=%.2f, maxw=%.2f\n",
                                i, k1, k2, k2 - k1, weight[i], q, dq, left, weight[i], totalWeight * scale.max(q, normalizer)));
            }
            q += dq / 2;
            left += weight[i];
            k1 = k2;
        }

        return badCount;
    }

    /**
     * Merges any pending inputs and compresses the data down to the public setting.
     * Note that this typically loses a bit of precision and thus isn't a thing to
     * be doing all the time. It is best done only when we want to show results to
     * the outside world.
     */
    @Override
    public void compress()
    {
        mergeNewValues(true, publicCompression);
    }

    @Override
    public long size()
    {
        return (long) (totalWeight + unmergedWeight);
    }

    @Override
    public double cdf(double x)
    {
        mergeNewValues();

        if (activeCentroids == 0) {
            // no data to examine
            return Double.NaN;
        }
        else if (activeCentroids == 1) {
            // exactly one centroid, should have max==min
            double width = max - min;
            if (x < min) {
                return 0;
            }
            else if (x > max) {
                return 1;
            }
            else if (x - min <= width) {
                // min and max are too close together to do any viable interpolation
                return 0.5;
            }
            else {
                // interpolate if somehow we have weight > 0 and max != min
                return (x - min) / (max - min);
            }
        }
        else {
            int n = activeCentroids;
            if (x < min) {
                return 0;
            }

            if (x > max) {
                return 1;
            }

            // check for the left tail
            if (x < mean[0]) {
                // note that this is different than mean[0] > min
                // ... this guarantees we divide by non-zero number and interpolation works
                if (mean[0] - min > 0) {
                    // must be a sample exactly at min
                    if (x == min) {
                        return 0.5 / totalWeight;
                    }
                    else {
                        return (1 + (x - min) / (mean[0] - min) * (weight[0] / 2 - 1)) / totalWeight;
                    }
                }
                else {
                    // this should be redundant with the check x < min
                    return 0;
                }
            }
            Preconditions.checkArgument(x >= mean[0], "error");

            // and the right tail
            if (x > mean[n - 1]) {
                if (max - mean[n - 1] > 0) {
                    if (x == max) {
                        return 1 - 0.5 / totalWeight;
                    }
                    else {
                        // there has to be a single sample exactly at max
                        double dq = (1 + (max - x) / (max - mean[n - 1]) * (weight[n - 1] / 2 - 1)) / totalWeight;
                        return 1 - dq;
                    }
                }
                else {
                    return 1;
                }
            }

            // we know that there are at least two centroids and mean[0] < x < mean[n-1]
            // that means that there are either one or more consecutive centroids all at exactly x
            // or there are consecutive centroids, c0 < x < c1
            double weightSoFar = 0;
            for (int it = 0; it < n - 1; it++) {
                // weightSoFar does not include weight[it] yet
                if (mean[it] == x) {
                    // we have one or more centroids == x, treat them as one
                    // dw will accumulate the weight of all of the centroids at x
                    double dw = 0;
                    while (it < n && mean[it] == x) {
                        dw += weight[it];
                        it++;
                    }
                    return (weightSoFar + dw / 2) / totalWeight;
                }
                else if (mean[it] <= x && x < mean[it + 1]) {
                    // landed between centroids ... check for floating point madness
                    if (mean[it + 1] - mean[it] > 0) {
                        // note how we handle singleton centroids here
                        // the point is that for singleton centroids, we know that their entire
                        // weight is exactly at the centroid and thus shouldn't be involved in
                        // interpolation
                        double leftExcludedW = 0;
                        double rightExcludedW = 0;
                        if (weight[it] == 1) {
                            if (weight[it + 1] == 1) {
                                // two singletons means no interpolation
                                // left singleton is in, right is out
                                return (weightSoFar + 1) / totalWeight;
                            }
                            else {
                                leftExcludedW = 0.5;
                            }
                        }
                        else if (weight[it + 1] == 1) {
                            rightExcludedW = 0.5;
                        }
                        double dw = (weight[it] + weight[it + 1]) / 2;

                        // can't have double singleton (handled that earlier)
                        Preconditions.checkArgument(dw > 1, "error");
                        Preconditions.checkArgument((leftExcludedW + rightExcludedW) <= 0.5, "error");

                        // adjust endpoints for any singleton
                        double left = mean[it];
                        double right = mean[it + 1];

                        double dwNoSingleton = dw - leftExcludedW - rightExcludedW;

                        // adjustments have only limited effect on endpoints
                        Preconditions.checkArgument(dwNoSingleton > dw / 2, "error");
                        Preconditions.checkArgument(right - left > 0, "error");

                        double base = weightSoFar + weight[it] / 2 + leftExcludedW;
                        return (base + dwNoSingleton * (x - left) / (right - left)) / totalWeight;
                    }
                    else {
                        // this is simply caution against floating point madness
                        // it is conceivable that the centroids will be different
                        // but too near to allow safe interpolation
                        double dw = (weight[it] + weight[it + 1]) / 2;
                        return (weightSoFar + dw) / totalWeight;
                    }
                }
                else {
                    weightSoFar += weight[it];
                }
            }
            if (x == mean[n - 1]) {
                return 1 - 0.5 / totalWeight;
            }
            else {
                throw new IllegalStateException("Can't happen ... loop fell through");
            }
        }
    }

    @Override
    public double quantile(double q)
    {
        if (q < 0 || q > 1) {
            throw new IllegalArgumentException("q should be in [0,1], got " + q);
        }
        mergeNewValues();

        if (activeCentroids == 0) {
            // no centroids means no data, no way to get a quantile
            return Double.NaN;
        }
        else if (activeCentroids == 1) {
            // with one data point, all quantiles lead to Rome
            return mean[0];
        }

        // we know that there are at least two centroids now
        int n = activeCentroids;

        // if values were stored in a sorted array, index would be the offset we are interested in
        final double index = q * totalWeight;

        // beyond the boundaries, we return min or max
        // usually, the first centroid will have unit weight so this will make it moot
        if (index < 1) {
            return min;
        }

        // if the left centroid has more than one sample, we still know
        // that one sample occurred at min so we can do some interpolation
        if (weight[0] > 1 && index < weight[0] / 2) {
            // there is a single sample at min so we interpolate with less weight
            return min + (index - 1) / (weight[0] / 2 - 1) * (mean[0] - min);
        }

        // usually the last centroid will have unit weight so this test will make it moot
        if (index > totalWeight - 1) {
            return max;
        }

        // if the right-most centroid has more than one sample, we still know
        // that one sample occurred at max so we can do some interpolation
        if (weight[n - 1] > 1 && totalWeight - index <= weight[n - 1] / 2) {
            return max - (totalWeight - index - 1) / (weight[n - 1] / 2 - 1) * (max - mean[n - 1]);
        }

        // in between extremes we interpolate between centroids
        double weightSoFar = weight[0] / 2;
        for (int i = 0; i < n - 1; i++) {
            double dw = (weight[i] + weight[i + 1]) / 2;
            if (weightSoFar + dw > index) {
                // centroids i and i+1 bracket our current point

                // check for unit weight
                double leftUnit = 0;
                if (weight[i] == 1) {
                    if (index - weightSoFar < 0.5) {
                        // within the singleton's sphere
                        return mean[i];
                    }
                    else {
                        leftUnit = 0.5;
                    }
                }
                double rightUnit = 0;
                if (weight[i + 1] == 1) {
                    if (weightSoFar + dw - index <= 0.5) {
                        // no interpolation needed near singleton
                        return mean[i + 1];
                    }
                    rightUnit = 0.5;
                }
                double z1 = index - weightSoFar - leftUnit;
                double z2 = weightSoFar + dw - index - rightUnit;
                return weightedAverage(mean[i], z2, mean[i + 1], z1);
            }
            weightSoFar += dw;
        }
        // we handled singleton at end up above
        Preconditions.checkArgument(weight[n - 1] > 1, "error");
        Preconditions.checkArgument(index <= totalWeight, "error");
        Preconditions.checkArgument(index >= totalWeight - weight[n - 1] / 2, "error");

        // weightSoFar = totalWeight - weight[n-1]/2 (very nearly)
        // so we interpolate out to max value ever seen
        double z1 = index - totalWeight - weight[n - 1] / 2.0;
        double z2 = weight[n - 1] / 2 - z1;
        return weightedAverage(mean[n - 1], z1, max, z2);
    }

    @Override
    public int centroidCount()
    {
        mergeNewValues();
        return activeCentroids;
    }

    @Override
    public Collection<Centroid> centroids()
    {
        // we don't actually keep centroid structures around so we have to fake it
        compress();
        return new AbstractCollection<Centroid>()
        {
            @Override
            public Iterator<Centroid> iterator()
            {
                return new Iterator<Centroid>()
                {
                    int i;

                    @Override
                    public boolean hasNext()
                    {
                        return i < activeCentroids;
                    }

                    @Override
                    public Centroid next()
                    {
                        Centroid rc = new Centroid(mean[i], (int) weight[i], data != null ? data.get(i) : null);
                        i++;
                        return rc;
                    }

                    @Override
                    public void remove()
                    {
                        throw new UnsupportedOperationException("Default operation");
                    }
                };
            }

            @Override
            public int size()
            {
                return activeCentroids;
            }
        };
    }

    @Override
    public double compression()
    {
        return publicCompression;
    }

    @Override
    public int byteSize()
    {
        compress();
        // format code, type code, min (double), max (double), compression(double), #centroids(int),
        // then two doubles per centroid
        return activeCentroids * 16 + 30;
    }

    @Override
    public int smallByteSize()
    {
        compress();
        // format code(int), compression(float), buffer-size(short), temp-size(short), #centroids-1(short),
        // then two floats per centroid
        return activeCentroids * 8 + 30;
    }

    @SuppressWarnings("WeakerAccess")
    public ScaleFunction getScaleFunction()
    {
        return scale;
    }

    public enum Encoding
    {
        VERBOSE_ENCODING((byte) 0), SMALL_ENCODING((byte) 1);

        private final byte code;

        Encoding(byte code)
        {
            this.code = code;
        }
    }

    @Override
    public Slice serialize()
    {
        SliceOutput sliceBuilder = new DynamicSliceOutput(byteSize());

        compress();
        sliceBuilder.writeByte(0); // version 0 of T-Digest serialization                     // 1
        sliceBuilder.writeByte(0); // represents the underlying data type of the distribution // + 1
        sliceBuilder.writeDouble(min);                                                              // + 8
        sliceBuilder.writeDouble(max);                                                              // + 8
        sliceBuilder.writeDouble(publicCompression);                                                // + 8
        sliceBuilder.writeInt(activeCentroids);                                                     // + 4 = 30
        for (int i = 0; i < activeCentroids; i++) {
            sliceBuilder.writeDouble(weight[i]);
            sliceBuilder.writeDouble(mean[i]);
        }

        return sliceBuilder.slice();
    }

    @SuppressWarnings("WeakerAccess")
    public static MergingDigest deserialize(Slice slice)
    {
        SliceInput sliceInput = new BasicSliceInput(slice);
        byte format = sliceInput.readByte();
        Preconditions.checkArgument(format == 0, "Invalid format");
        byte type = sliceInput.readByte();
        Preconditions.checkArgument(type == 0, "Invalid type; expecting '0' (type double)");
        double min = sliceInput.readDouble();
        double max = sliceInput.readDouble();
        double compression = sliceInput.readDouble();
        int activeCentroids = sliceInput.readInt();
        MergingDigest md = new MergingDigest(compression);
        md.setMinMax(min, max);
        md.activeCentroids = activeCentroids;
        for (int i = 0; i < activeCentroids; i++) {
            md.weight[i] = sliceInput.readDouble();
            md.mean[i] = sliceInput.readDouble();

            md.totalWeight += md.weight[i];
        }
        sliceInput.close();
        return md;
    }

    @Override
    public String toString()
    {
        return "MergingDigest"
                + "-" + getScaleFunction()
                + "-" + (useWeightLimit ? "weight" : "kSize")
                + "-" + (useAlternatingSort ? "alternating" : "stable")
                + "-" + (useTwoLevelCompression ? "twoLevel" : "oneLevel");
    }
}
