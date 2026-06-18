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

package com.facebook.presto.tdigest;

import com.facebook.airlift.concurrent.NotThreadSafe;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.tdigest.TDigestUtils.maxSize;
import static com.facebook.presto.tdigest.TDigestUtils.normalizer;
import static com.facebook.presto.tdigest.TDigestUtils.reverse;
import static com.facebook.presto.tdigest.TDigestUtils.sort;
import static com.facebook.presto.tdigest.TDigestUtils.weightedAverage;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.Slices.wrappedDoubleArray;
import static java.lang.Double.isNaN;
import static java.lang.Math.ceil;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.arraycopy;
import static java.util.Collections.shuffle;

/**
 * Maintains a t-digest by collecting new points in a buffer that is then sorted occasionally and merged
 * into a sorted array that contains previously computed centroids.
 * <p>
 * This can be very fast because the cost of sorting and merging is amortized over several insertion. If
 * we keep N centroids total and have the input array of size k, then the amortized cost is approximately
 * <p>
 * N/k + log k
 * <p>
 * These costs even out when N/k = log k.  Balancing costs is often a good place to start in optimizing an
 * algorithm.
 * The virtues of this kind of t-digest implementation include:
 * <ul>
 * <li>No allocation is required after initialization</li>
 * <li>The data structure automatically compresses existing centroids when possible</li>
 * <li>No Java object overhead is incurred for centroids since data is kept in primitive arrays</li>
 * </ul>
 * <p>
 */

@NotThreadSafe
public class TDigest
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(TDigest.class).instanceSize();
    static final double MAX_COMPRESSION_FACTOR = 1_000;
    private static final double sizeFudge = 30;
    private static final double EPSILON = 0.001;

    private double min = Double.POSITIVE_INFINITY;
    private double max = Double.NEGATIVE_INFINITY;
    private double sum;

    private final Random gen = ThreadLocalRandom.current();

    private int mergeCount;

    private final double publicCompression;
    private final double compression;
    private final int maxCentroidCount;
    private final int maxBufferSize;

    // points to the first unused centroid
    private int activeCentroids;
    private double totalWeight;
    private double[] weight;
    private double[] mean;

    // this is the index of the next temporary centroid
    // this is a more Java-like convention than activeCentroids uses
    private int tempUsed;
    private double unmergedWeight;
    private double[] tempWeight;
    private double[] tempMean;

    // array used for sorting the temp centroids
    // to avoid allocations during operation
    private int[] order;

    private TDigest(double compression)
    {
        // ensure compression >= 10
        // default maxCentroidCount = 2 * ceil(compression)
        // default maxBufferSize = 5 * maxCentroidCount
        // ensure maxCentroidCount > 2 * compression + weightLimitFudge
        // ensure maxBufferSize > 2 * maxCentroidCount
        checkArgument(!isNaN(compression));
        checkArgument(compression <= MAX_COMPRESSION_FACTOR, "Compression factor cannot exceed %s", MAX_COMPRESSION_FACTOR);
        this.publicCompression = max(compression, 10);
        // publicCompression is how many centroids the user asked for
        // compression is how many we actually keep
        this.compression = 2 * this.publicCompression;

        // having a big buffer is good for speed
        this.maxBufferSize = 5 * (int) ceil(this.compression + sizeFudge);
        this.maxCentroidCount = (int) ceil(this.compression + sizeFudge);

        this.weight = new double[1];
        this.mean = new double[1];
        this.tempWeight = new double[1];
        this.tempMean = new double[1];
        this.order = new int[1];

        this.activeCentroids = 0;
    }

    public static TDigest createTDigest(double compression)
    {
        return new TDigest(compression);
    }

    public static TDigest createTDigest(
            double[] centroidMeans,
            double[] centroidWeights,
            double compression,
            double min,
            double max,
            double sum,
            int count)
    {
        TDigest tDigest = new TDigest(compression);
        tDigest.setMinMax(min, max);
        tDigest.setSum(sum);
        tDigest.totalWeight = Arrays.stream(centroidWeights).sum(); // set totalWeight to sum of all centroidWeights
        tDigest.activeCentroids = count;
        tDigest.weight = centroidWeights;
        tDigest.mean = centroidMeans;

        return tDigest;
    }

    public static TDigest createTDigest(Slice slice)
    {
        if (slice == null) {
            return null;
        }

        SliceInput sliceInput = new BasicSliceInput(slice);
        try {
            byte format = sliceInput.readByte();
            checkArgument(format == 0 || format == 1, "Invalid serialization format for TDigest; expected '0' or '1'");
            byte type = sliceInput.readByte();
            checkArgument(type == 0, "Invalid type for TDigest; expected '0' (type double)");
            double min = sliceInput.readDouble();
            double max = sliceInput.readDouble();
            double sum = format == 1 ? sliceInput.readDouble() : 0.0;
            double publicCompression = max(10, sliceInput.readDouble());
            TDigest r = new TDigest(publicCompression);
            r.setMinMax(min, max);
            r.setSum(sum);
            r.totalWeight = sliceInput.readDouble();
            r.activeCentroids = sliceInput.readInt();
            r.weight = new double[r.activeCentroids];
            r.mean = new double[r.activeCentroids];
            sliceInput.readBytes(wrappedDoubleArray(r.weight), r.activeCentroids * SIZE_OF_DOUBLE);
            sliceInput.readBytes(wrappedDoubleArray(r.mean), r.activeCentroids * SIZE_OF_DOUBLE);

            // Validate deserialized TDigest data
            for (int i = 0; i < r.activeCentroids; i++) {
                checkArgument(!isNaN(r.mean[i]), "Deserialized t-digest contains NaN mean value");
                checkArgument(r.weight[i] > 0, "weight must be > 0");
            }

            sliceInput.close();
            return r;
        }
        catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException("Incorrect slice serialization format");
        }
    }

    public void add(double x)
    {
        add(x, 1);
    }

    public void add(double x, double w)
    {
        checkArgument(!isNaN(x), "Cannot add NaN to t-digest");
        checkArgument(w > 0L, "weight must be > 0");

        if (tempWeight.length == tempUsed) {
            tempWeight = Arrays.copyOf(tempWeight, min(tempWeight.length * 2, maxBufferSize));
            tempMean = Arrays.copyOf(tempMean, min(tempMean.length * 2, maxBufferSize));
            order = Arrays.copyOf(order, min(order.length * 2, maxBufferSize));
        }

        if (tempUsed >= maxBufferSize - activeCentroids - 1) {
            mergeNewValues();
        }

        int where = tempUsed++;
        tempWeight[where] = w;
        tempMean[where] = x;
        unmergedWeight += w;
        if (x < min) {
            min = x;
        }
        if (x > max) {
            max = x;
        }
        sum += (x * w);
    }

    public void merge(TDigest other)
    {
        checkArgument(other != null, "Cannot merge with a null t-digest");
        checkArgument(this.publicCompression == other.getCompressionFactor(), "TDigests must have the same compression, found (%s, %s)", this.publicCompression,
                other.getCompressionFactor());
        List<Centroid> tmp = new ArrayList<>();
        for (Centroid centroid : other.centroids()) {
            tmp.add(centroid);
        }

        shuffle(tmp, gen);
        for (Centroid centroid : tmp) {
            add(centroid.getMean(), centroid.getWeight());
        }
    }

    private void mergeNewValues()
    {
        mergeNewValues(false, compression);
    }

    private void mergeNewValues(boolean force, double compression)
    {
        if (unmergedWeight == 0) {
            return;
        }

        if (force || unmergedWeight > 0) {
            // ensure buffer is big enough to hold current centroids and incoming values
            if (activeCentroids + tempUsed >= tempWeight.length) {
                tempWeight = Arrays.copyOf(tempWeight, min(max(tempWeight.length * 2, activeCentroids + tempUsed + 1), maxBufferSize));
                tempMean = Arrays.copyOf(tempMean, min(max(tempMean.length * 2, activeCentroids + tempUsed + 1), maxBufferSize));
                order = Arrays.copyOf(order, min(max(order.length * 2, activeCentroids + tempUsed + 1), maxBufferSize));
            }
            // note that we run the merge in reverse every other merge to avoid left-to-right bias in merging
            merge(tempMean, tempWeight, tempUsed, order, unmergedWeight, mergeCount % 2 == 1, compression);
            mergeCount++;
            tempUsed = 0;
            unmergedWeight = 0;
        }
    }

    private void merge(double[] incomingMean,
            double[] incomingWeight,
            int incomingCount,
            int[] incomingOrder,
            double unmergedWeight,
            boolean runBackwards,
            double compression)
    {
        arraycopy(mean, 0, incomingMean, incomingCount, activeCentroids);
        arraycopy(weight, 0, incomingWeight, incomingCount, activeCentroids);
        incomingCount += activeCentroids;

        checkArgument(incomingOrder != null, "Incoming order array was null");
        sort(incomingOrder, incomingMean, incomingCount);

        if (runBackwards) {
            reverse(incomingOrder, 0, incomingCount);
        }

        totalWeight += unmergedWeight;

        checkArgument((activeCentroids + incomingCount) > 0, "Active centroids plus incoming count must be > 0, was %s", activeCentroids + incomingCount);
        activeCentroids = 0;
        mean[activeCentroids] = incomingMean[incomingOrder[0]];
        weight[activeCentroids] = incomingWeight[incomingOrder[0]];
        double weightSoFar = 0;

        double normalizer = normalizer(compression, totalWeight);
        for (int i = 1; i < incomingCount; i++) {
            int ix = incomingOrder[i];
            double proposedWeight = weight[activeCentroids] + incomingWeight[ix];

            double q0 = weightSoFar / totalWeight;
            double q2 = (weightSoFar + proposedWeight) / totalWeight;

            if (proposedWeight <= totalWeight * min(maxSize(q0, normalizer), maxSize(q2, normalizer))) {
                // next point can be merged into existing centroid
                weight[activeCentroids] += incomingWeight[ix];
                mean[activeCentroids] = mean[activeCentroids] + (incomingMean[ix] - mean[activeCentroids]) * incomingWeight[ix] / weight[activeCentroids];
                incomingWeight[ix] = 0;
            }
            else {
                // move to next output, copy out first centroid
                weightSoFar += weight[activeCentroids];

                activeCentroids++;
                if (mean.length == activeCentroids) {
                    mean = Arrays.copyOf(mean, min(mean.length * 2, maxCentroidCount));
                    weight = Arrays.copyOf(weight, min(weight.length * 2, maxCentroidCount));
                }
                mean[activeCentroids] = incomingMean[ix];
                weight[activeCentroids] = incomingWeight[ix];
                incomingWeight[ix] = 0;
            }
        }
        activeCentroids++;
        if (mean.length == activeCentroids) {
            mean = Arrays.copyOf(mean, min(mean.length * 2, maxCentroidCount));
            weight = Arrays.copyOf(weight, min(weight.length * 2, maxCentroidCount));
        }

        // sanity check
        double sumWeights = 0;
        for (int i = 0; i < activeCentroids; i++) {
            sumWeights += weight[i];
        }

        checkArgument(Math.abs(sumWeights - totalWeight) < EPSILON, "Sum must equal the total weight, but sum:%s != totalWeight:%s", sumWeights, totalWeight);
        if (runBackwards) {
            reverse(mean, 0, activeCentroids);
            reverse(weight, 0, activeCentroids);
        }

        if (totalWeight > 0) {
            min = min(min, mean[0]);
            max = max(max, mean[activeCentroids - 1]);
        }
    }

    /**
     * Merges any pending inputs and compresses the data down to the public setting.
     */
    public void compress()
    {
        mergeNewValues(true, publicCompression);
    }

    public double getSize()
    {
        return totalWeight + unmergedWeight;
    }

    public double getCdf(double x)
    {
        if (unmergedWeight > 0) {
            compress();
        }

        if (activeCentroids == 0) {
            return Double.NaN;
        }
        if (activeCentroids == 1) {
            double width = max - min;
            if (x < min) {
                return 0;
            }
            if (x > max) {
                return 1;
            }
            if (x - min <= width) {
                // min and max are too close together to do any viable interpolation
                return 0.5;
            }
            return (x - min) / (max - min);
        }
        int n = activeCentroids;
        if (x < min) {
            return 0;
        }

        if (x > max) {
            return 1;
        }

        // check for the left tail
        if (x < mean[0]) {
            // guarantees we divide by non-zero number and interpolation works
            if (mean[0] - min > 0) {
                // must be a sample exactly at min
                if (x == min) {
                    return 0.5 / totalWeight;
                }
                return (1 + (x - min) / (mean[0] - min) * (weight[0] / 2 - 1)) / totalWeight;
            }
            return 0;
        }
        checkArgument(x >= mean[0], "Value x:%s must be greater than mean of first centroid %s if we got here", x, mean[0]);

        // and the right tail
        if (x > mean[n - 1]) {
            if (max - mean[n - 1] > 0) {
                if (x == max) {
                    return 1 - 0.5 / totalWeight;
                }
                // there has to be a single sample exactly at max
                double dq = (1 + (max - x) / (max - mean[n - 1]) * (weight[n - 1] / 2 - 1)) / totalWeight;
                return 1 - dq;
            }
            return 1;
        }

        // we know that there are at least two centroids and mean[0] < x < mean[n-1]
        // that means that there are either one or more consecutive centroids all at exactly x
        // or there are consecutive centroids, c0 < x < c1
        double weightSoFar = 0;
        for (int it = 0; it < n - 1; it++) {
            // weightSoFar does not include weight[it] yet
            if (mean[it] == x) {
                // dw will accumulate the weight of all of the centroids at x
                double dw = 0;
                while (it < n && mean[it] == x) {
                    dw += weight[it];
                    it++;
                }
                return (weightSoFar + dw / 2) / totalWeight;
            }
            else if (mean[it] <= x && x < mean[it + 1]) {
                // landed between centroids
                if (mean[it + 1] - mean[it] > 0) {
                    // no interpolation needed if we have a singleton centroid
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

                    checkArgument(dw > 1, "dw must be > 1, was %s", dw);
                    checkArgument((leftExcludedW + rightExcludedW) <= 0.5, "Excluded weight must be <= 0.5, was %s", leftExcludedW + rightExcludedW);

                    // adjust endpoints for any singleton
                    double left = mean[it];
                    double right = mean[it + 1];

                    double dwNoSingleton = dw - leftExcludedW - rightExcludedW;

                    checkArgument(right - left > 0, "Centroids should be in ascending order, but mean of left centroid was greater than right centroid");

                    double base = weightSoFar + weight[it] / 2 + leftExcludedW;
                    return (base + dwNoSingleton * (x - left) / (right - left)) / totalWeight;
                }
                else {
                    // caution against floating point madness
                    double dw = (weight[it] + weight[it + 1]) / 2;
                    return (weightSoFar + dw) / totalWeight;
                }
            }
            else {
                weightSoFar += weight[it];
            }
        }
        checkArgument(x == mean[n - 1], "At this point, x must equal the mean of the last centroid");

        return 1 - 0.5 / totalWeight;
    }

    public double getQuantile(double q)
    {
        checkArgument(q >= 0 && q <= 1, "q should be in [0,1], got %s", q);
        if (unmergedWeight > 0) {
            compress();
        }

        if (activeCentroids == 0) {
            return Double.NaN;
        }
        else if (activeCentroids == 1) {
            return mean[0];
        }

        int n = activeCentroids;

        final double index = q * totalWeight;

        if (index < 1) {
            return min;
        }

        // if the left centroid has more than one sample, we still know
        // that one sample occurred at min so we can do some interpolation
        if (weight[0] > 1 && index < weight[0] / 2) {
            // there is a single sample at min so we interpolate with less weight
            return min + (index - 1) / (weight[0] / 2 - 1) * (mean[0] - min);
        }

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
            // centroids i and i + 1 bracket our current point
            double dw = (weight[i] + weight[i + 1]) / 2;
            if (weightSoFar + dw > index) {
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

        checkArgument(weight[n - 1] > 1, "Expected weight[n - 1] > 1, but was %s", weight[n - 1]);
        checkArgument(index <= totalWeight, "Expected index <= totalWeight, but index:%s > totalWeight:%s", index, totalWeight);
        checkArgument(index >= totalWeight - weight[n - 1] / 2, "Expected index >= totalWeight - weight[n - 1] / 2, but " +
                "index:%s < %s", index, totalWeight - weight[n - 1] / 2);

        // weightSoFar = totalWeight - weight[n - 1] / 2 (very nearly)
        // so we interpolate out to max value ever seen
        double z1 = index - totalWeight - weight[n - 1] / 2.0;
        double z2 = weight[n - 1] / 2 - z1;
        return weightedAverage(mean[n - 1], z1, max, z2);
    }

    public double trimmedMean(double lowerQuantileBound, double upperQuantileBound)
    {
        checkArgument(lowerQuantileBound >= 0 && lowerQuantileBound <= 1, "lowerQuantileBound should be in [0,1], got %s", lowerQuantileBound);
        checkArgument(upperQuantileBound >= 0 && upperQuantileBound <= 1, "upperQuantileBound should be in [0,1], got %s", upperQuantileBound);

        if (unmergedWeight > 0) {
            compress();
        }

        if (activeCentroids == 0 || lowerQuantileBound >= upperQuantileBound) {
            return Double.NaN;
        }

        if (lowerQuantileBound == 0 && upperQuantileBound == 1) {
            return sum / totalWeight;
        }

        double lowerIndex = lowerQuantileBound * totalWeight;
        double upperIndex = upperQuantileBound * totalWeight;

        double weightSoFar = 0;
        double sumInBounds = 0;
        double weightInBounds = 0;
        for (int i = 0; i < activeCentroids; i++) {
            if (weightSoFar < lowerIndex && lowerIndex <= weightSoFar + weight[i] && upperIndex <= weightSoFar + weight[i]) {
                // lower and upper bounds are so close together that they are in the same weight interval
                return mean[i];
            }
            else if (weightSoFar < lowerIndex && lowerIndex <= weightSoFar + weight[i]) {
                // the lower bound is between our current point and the next point
                double addedWeight = weightSoFar + weight[i] - lowerIndex;
                sumInBounds += mean[i] * addedWeight;
                weightInBounds += addedWeight;
            }
            else if (upperIndex < weightSoFar + weight[i] && upperIndex > weightSoFar) {
                // the upper bound is between our current point and the next point
                double addedWeight = upperIndex - weightSoFar;
                sumInBounds += mean[i] * addedWeight;
                weightInBounds += addedWeight;
                return sumInBounds / weightInBounds;
            }
            else if (lowerIndex <= weightSoFar && weightSoFar <= upperIndex) {
                // we are somewhere in between the lower and upper bounds
                sumInBounds += mean[i] * weight[i];
                weightInBounds += weight[i];
            }
            weightSoFar += weight[i];
        }

        return sumInBounds / weightInBounds;
    }

    public int centroidCount()
    {
        mergeNewValues();
        return activeCentroids;
    }

    public Collection<Centroid> centroids()
    {
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
                        Centroid rc = new Centroid(mean[i], weight[i]);
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

    public double getCompressionFactor()
    {
        return publicCompression;
    }

    public long estimatedSerializedSizeInBytes()
    {
        compress();
        return SIZE_OF_BYTE                             // format
                + SIZE_OF_BYTE                          // type (e.g double, float, bigint)
                + SIZE_OF_DOUBLE                        // min
                + SIZE_OF_DOUBLE                        // max
                + SIZE_OF_DOUBLE                        // sum
                + SIZE_OF_DOUBLE                        // compression factor
                + SIZE_OF_DOUBLE                        // total weight
                + SIZE_OF_INT                           // number of centroids
                + SIZE_OF_DOUBLE * activeCentroids      // weight[], containing weight of each centroid
                + SIZE_OF_DOUBLE * activeCentroids;     // mean[], containing mean of each centroid
    }

    public long estimatedInMemorySizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(weight) + sizeOf(mean) + sizeOf(tempWeight) + sizeOf(tempMean) + sizeOf(order);
    }

    public Slice serialize()
    {
        // Validate data before serialization
        for (int i = 0; i < activeCentroids; i++) {
            checkArgument(!isNaN(mean[i]), "Cannot serialize t-digest with NaN mean value");
            checkArgument(weight[i] > 0, "Cannot serialize t-digest with non-positive weight");
        }

        SliceOutput sliceOutput = new DynamicSliceOutput(toIntExact(estimatedSerializedSizeInBytes()));

        sliceOutput.writeByte(1); // version 1 of T-Digest serialization
        sliceOutput.writeByte(0); // represents the underlying data type of the distribution
        sliceOutput.writeDouble(min);
        sliceOutput.writeDouble(max);
        sliceOutput.writeDouble(sum);
        sliceOutput.writeDouble(publicCompression);
        sliceOutput.writeDouble(totalWeight);
        sliceOutput.writeInt(activeCentroids);
        sliceOutput.writeBytes(wrappedDoubleArray(weight), 0, activeCentroids * SIZE_OF_DOUBLE);
        sliceOutput.writeBytes(wrappedDoubleArray(mean), 0, activeCentroids * SIZE_OF_DOUBLE);
        return sliceOutput.slice();
    }

    private void setMinMax(double min, double max)
    {
        this.min = min;
        this.max = max;
    }

    private void setSum(double sum)
    {
        this.sum = sum;
    }

    public double getMin()
    {
        return min;
    }

    public double getMax()
    {
        return max;
    }

    public double getSum()
    {
        return sum;
    }

    /**
     * Scale all the counts by the given scale factor.
     */
    public void scale(double scaleFactor)
    {
        checkArgument(scaleFactor > 0, "scale factor must be > 0");
        // Compress the scaled digest.
        compress();

        // Scale all the counts.
        for (int i = 0; i < weight.length; i++) {
            weight[i] *= scaleFactor;
        }
        totalWeight *= scaleFactor;
        sum *= scaleFactor;
    }

    public String toString()
    {
        return format("TDigest%nCompression:%s%nCentroid Count:%s%nSize:%s%nMin:%s Median:%s Max:%s",
                publicCompression, activeCentroids, totalWeight, min, getQuantile(0.5), max);
    }
}
