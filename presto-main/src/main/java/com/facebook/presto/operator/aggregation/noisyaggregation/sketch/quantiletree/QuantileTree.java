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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree;

import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.SIZE_OF_BYTE;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

/**
 * The QuantileTree represents a hierarchical histogram over the floating-point range [rangeLowerBound, rangeUpperBound].
 * This hierarchy is represented with a tree structure. The bottom level of the tree holds the canonical histogram bins.
 * Each level up in the hierarchy, a node reflects the sum of its descendants.
 * This seemingly redundant structure is a common structure used to handle range queries under differential privacy.
 * See <a href="https://fb.workplace.com/notes/147894314581680">this Workplace note</a> for background.
 * <p>
 * To save space, some histogram levels may be stored verbatim, while others may be sketched using the PrivateCountMedianSketch.
 * Privacy is obtained by adding Gaussian noise to the nodes of the tree (or their sketched values), and privacy is parameterized
 * by rho, according to zero-concentrated DP. The privacy guarantee holds under unbounded-neighbors (add/remove) semantics,
 * for multisets differing by one item.
 */
public class QuantileTree
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(QuantileTree.class).instanceSize();
    private static final int MIN_BRANCHING_FACTOR = 2;
    public static final double NON_PRIVATE_RHO = Double.POSITIVE_INFINITY;
    private final double rangeLowerBound;
    private final double rangeUpperBound;
    private final int binCount;
    private final int branchingFactor;
    private final QuantileTreeLevel[] levels;
    private double rho = NON_PRIVATE_RHO;

    public QuantileTree(double rangeLowerBound, double rangeUpperBound, int binCount, int branchingFactor, int sketchDepth, int sketchWidth)
    {
        checkArgument(rangeLowerBound < rangeUpperBound, "rangeLowerBound must be less than rangeUpperBound");
        checkArgument(binCount >= MIN_BRANCHING_FACTOR, "bin count must be at least %s", MIN_BRANCHING_FACTOR);
        checkArgument(branchingFactor >= MIN_BRANCHING_FACTOR && branchingFactor <= binCount,
                "branching factor must be between %s and %s", MIN_BRANCHING_FACTOR, binCount);
        checkArgument(sketchDepth > 0 && sketchWidth > 0, "sketchDepth and sketchWidth must be positive");

        this.rangeLowerBound = rangeLowerBound;
        this.rangeUpperBound = rangeUpperBound;
        this.branchingFactor = branchingFactor;

        // Build the tree!
        // We force the tree to be a complete tree, stopping when the bin count is at least the requested binCount.
        // (In other words, the actual bin count will be rounded up the next power of branchingFactor.)
        // Each level of the tree will be represented by an exact histogram if the histogram is smaller than the
        // requested sketch dimensions. Otherwise, we represent the level with a SketchedLevel (CountSketch).
        int actualBinCount = 1; // The first level is the root node.
        int totalLevels = 1;
        while (actualBinCount < binCount) {
            actualBinCount *= this.branchingFactor;
            totalLevels++;
        }
        this.binCount = actualBinCount;

        levels = new QuantileTreeLevel[totalLevels];
        long hypotheticalSketchSize = sketchDepth * sketchWidth;
        int binCountAtCurrentLevel = 1;
        for (int i = 0; i < totalLevels; i++) {
            // add the smaller of the two options: a HistogramLevel or a SketchedLevel
            if (binCountAtCurrentLevel <= hypotheticalSketchSize) {
                levels[i] = new HistogramLevel(binCountAtCurrentLevel);
            }
            else {
                levels[i] = new SketchedLevel(sketchDepth, sketchWidth);
            }
            binCountAtCurrentLevel *= this.branchingFactor;
        }
    }

    public QuantileTree(double rangeLowerBound, double rangeUpperBound, int binCount, int branchingFactor, double rho, QuantileTreeLevel[] levels)
    {
        checkArgument(rangeLowerBound < rangeUpperBound, "rangeLowerBound must be less than rangeUpperBound");
        checkArgument(branchingFactor >= MIN_BRANCHING_FACTOR && branchingFactor <= binCount,
                "branching factor must be between %s and %s", MIN_BRANCHING_FACTOR, binCount);
        checkArgument(rho > 0, "rho must be positive");
        checkArgument(levels.length > 0, "levels can not be empty");

        this.rangeLowerBound = rangeLowerBound;
        this.rangeUpperBound = rangeUpperBound;
        this.binCount = binCount;
        this.branchingFactor = branchingFactor;
        this.rho = rho;

        int totalLevels = levels.length;
        this.levels = new QuantileTreeLevel[totalLevels];
        System.arraycopy(levels, 0, this.levels, 0, totalLevels);
    }

    public static QuantileTree deserialize(Slice serialized)
    {
        SliceInput s = new BasicSliceInput(serialized);
        double rangeLowerBound = s.readDouble();
        double rangeUpperBound = s.readDouble();
        double rho = s.readDouble();
        int binCount = s.readInt();
        int branchingFactor = s.readInt();
        int totalLevels = s.readByte();
        QuantileTreeLevel[] levels = new QuantileTreeLevel[totalLevels];

        // deserialize tree levels
        for (int i = 0; i < totalLevels; i++) {
            int levelFormat = s.readByte();

            if (levelFormat == QuantileTreeLevelFormat.SKETCHED.getTag()) {
                levels[i] = new SketchedLevel(s);
            }
            else if (levelFormat == QuantileTreeLevelFormat.HISTOGRAM.getTag()) {
                levels[i] = new HistogramLevel(s);
            }
        }

        return new QuantileTree(rangeLowerBound, rangeUpperBound, binCount, branchingFactor, rho, levels);
    }

    public void merge(QuantileTree other)
    {
        checkArgument(rangeLowerBound == other.rangeLowerBound && rangeUpperBound == other.rangeUpperBound,
                "Cannot merge quantiletree due to range mismatch");
        checkArgument(binCount == other.binCount,
                "Cannot merge quantiletree due to bin count mismatch");
        checkArgument(totalLevels() == other.totalLevels(), "Cannot merge quantiletree with different level structure");

        rho = mergedRho(rho, other.rho);
        for (int i = 0; i < levels.length; i++) {
            levels[i].merge(other.levels[i]);
        }
    }

    /**
     * The effective value of rho after merging two trees.
     * Note: this captures the noise level of the trees, not the privacy leakage.
     * In other words, the level of noise in the merged tree is the same as if everything
     * had been inserted into one tree with mergedRho(rho1, rho2).
     * <p>
     * Every counter in the quantiletree (whether a histogram bin or a sketched counter)
     * is given noise with variance proportional to 1 / rho. When merging trees, we sum
     * these counters, so the variances add. This gives an effective "merged rho" of
     * 1 / (1 / rho_1 + 1 / rho_2).
     */
    @VisibleForTesting
    static double mergedRho(double rho1, double rho2)
    {
        // Merging with a non-private tree? Take the other tree's rho value, as we're not adding any noise.
        if (rho1 == NON_PRIVATE_RHO) {
            return rho2;
        }
        if (rho2 == NON_PRIVATE_RHO) {
            return rho1;
        }

        // Merging two noisy trees returns something noisier.
        // This calculation comes from the addition of variances that are inversely proportional to rho1, rho2.
        return 1 / (1 / rho1 + 1 / rho2);
    }

    public Slice serialize()
    {
        SliceOutput res = new DynamicSliceOutput(estimatedSerializedSizeInBytes());
        res.appendDouble(this.rangeLowerBound); // 8
        res.appendDouble(this.rangeUpperBound); // 8
        res.appendDouble(this.rho); // 8
        res.appendInt(this.binCount); // 4
        res.appendInt(this.branchingFactor); // 4
        res.appendByte(this.totalLevels()); // 1

        for (QuantileTreeLevel level : levels) {
            res.appendByte(level.getFormat().getTag());
            res.appendBytes(level.serialize());
        }

        return res.slice();
    }

    public int getBranchingFactor()
    {
        return branchingFactor;
    }

    /**
     * The QuantileTree represents a hierarchical histogram over the range [rangeLowerBound, rangeUpperBound].
     * At the bottom level of the tree, each bin is equally sized over this range.
     * This function returns those bottom-level bin widths.
     * (Higher levels simply reflect the unions of these bottom-level bins.)
     */
    @VisibleForTesting
    double getBinWidth()
    {
        return (rangeUpperBound - rangeLowerBound) / binCount;
    }

    /**
     * Assigns a given value to one of the bins of the (hierarchical) histogram.
     * Specifically, this maps double values from the range [rangeLowerBound, rangeUpperBound]
     * to the discrete set { 0, 1, 2, ... binCount - 1 }.
     */
    @VisibleForTesting
    long toBin(double x)
    {
        long y = (long) Math.floor((x - rangeLowerBound) / getBinWidth());
        return Math.max(0, Math.min(binCount - 1, y));
    }

    /**
     * Maps values from discrete bins back to the original data range.
     * Due to discretization, we don't know where in a bin a given value falls.
     * We assume the midpoint of each bin.
     */
    @VisibleForTesting
    double fromBin(long y)
    {
        return rangeLowerBound + (y + 0.5) * getBinWidth();
    }

    public void add(double value)
    {
        long item = toBin(value);
        for (int i = totalLevels() - 1; i >= 0; i--) {
            levels[i].add(item);
            item = Math.floorDiv(item, Long.valueOf(branchingFactor));
        }
    }

    /**
     * Returns the (approximate) number of items in the QuantileTree
     */
    public double cardinality()
    {
        return new QuantileTreeSearch(this).cardinality();
    }

    /**
     * Find a value that corresponds to the p-th quantile
     * Uses a tree-traversal search algorithm with some inverse-variance weighting
     * for fast but not quite optimal estimation.
     */
    public double quantile(double p)
    {
        checkArgument(p >= 0 && p <= 1, "cumulative probability (p) must be between 0 and 1");
        long bin = new QuantileTreeSearch(this).findQuantile(p);
        return fromBin(bin);
    }

    public boolean isPrivacyEnabled()
    {
        return rho < NON_PRIVATE_RHO;
    }

    /**
     * Adds noise to the sketch to bring rho to the requested level
     */
    public void enablePrivacy(double targetRho, RandomizationStrategy randomizationStrategy)
    {
        // find the amount of noise needed to bring rho up to targetRho
        double rhoToAdd = findRho2(getRho(), targetRho);

        if (rhoToAdd == NON_PRIVATE_RHO) {
            // no additional noise needed
            return;
        }

        // divide rhoToAdd over the levels of the sketch
        for (QuantileTreeLevel level : levels) {
            level.addNoise(rhoToAdd / levels.length, randomizationStrategy);
        }

        this.rho = targetRho;
    }

    /**
     * Noise is added to every counter in the sketch with variance proportional to 1 / rho.
     * To raise the noise level, we re-add noise. Since the variance adds, the amount of noise
     * to add can be determined using the same math as in mergedRho(rho1, rho2). In this case,
     * however, we know rho1 and the final noise level (mergedRho), and we're solving for rho2.
     */
    @VisibleForTesting
    static double findRho2(double rho1, double targetRho)
    {
        checkArgument(rho1 >= targetRho, "existing rho is stricter than targetRho: %s < %s", rho1, targetRho);
        checkArgument(targetRho > 0, "targetRho must be positive");

        if (rho1 == NON_PRIVATE_RHO) {
            // if there is no noise in the existing sketch, just use the requested rho
            // (this includes the case when targetRho == NON_PRIVATE_RHO)
            return targetRho;
        }
        else {
            // targetRho = 1 / (1 / rho1 + 1 / rho2) -- see mergedRho(rho1, rho2)
            double rho2 = 1 / (1 / targetRho - 1 / rho1);
            return Math.max(0, rho2);
        }
    }

    /**
     * Returns a given tree level, indexed from the top of the tree.
     * Note: this "tree" has no root node, so the 0-th level will have two nodes, 1-st will have four, and so on.
     * This is in contrast with the QuantileTreeSearch class, which considers a virtual root node and indexes levels
     * starting with 0 for the (nonexistent) root node.
     */
    public QuantileTreeLevel getLevel(int index)
    {
        return this.levels[index];
    }

    public int totalLevels()
    {
        return levels.length;
    }

    public double getRho()
    {
        return this.rho;
    }

    /**
     * A mechanism satisfying rho-zCDP satisfies (rho + 2 sqrt(rho*log(1/delta)), delta)-DP for any delta > 0,
     * per <a href="https://arxiv.org/pdf/1605.02065.pdf">Bun and Steinke</a>.
     * <p>
     * Thus to satisfy (epsilon, delta)-DP for our choice of epsilon and delta, we obtain:
     * <p>
     * rho = epsilon - 2 * log(delta) - 2 * sqrt(log^2(delta) - epsilon * log(delta))
     */
    public static double getRhoForEpsilonDelta(double epsilon, double delta)
    {
        checkArgument(epsilon > 0, "epsilon must be positive");
        checkArgument(delta > 0, "delta must be positive");

        // An epsilon of infinity or a delta >= 1 indicates no privacy guarantee at all.
        if (epsilon == Double.POSITIVE_INFINITY || delta >= 1) {
            return NON_PRIVATE_RHO;
        }

        return epsilon - 2 * Math.log(delta) - 2 * Math.sqrt(Math.pow(Math.log(delta), 2) - epsilon * Math.log(delta));
    }

    public int estimatedSerializedSizeInBytes()
    {
        int size = 3 * SIZE_OF_DOUBLE + 2 * SIZE_OF_INT + SIZE_OF_BYTE; // lower, upper, rho, binCount, branchingFactor, totalLevels
        for (QuantileTreeLevel level : levels) {
            size += SIZE_OF_BYTE; // format tag of level
            size += level.estimatedSerializedSizeInBytes(); // level serialization
        }
        return size;
    }

    // optimize the loop calculation: TODO T139787526
    public long estimatedSizeInBytes()
    {
        long treeLevelsSize = 0;
        for (QuantileTreeLevel level : levels) {
            treeLevelsSize += level.estimatedSizeInBytes();
        }
        return INSTANCE_SIZE + SizeOf.sizeOf(levels) + treeLevelsSize;
    }
}
