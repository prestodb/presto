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
package com.facebook.presto.operator.aggregation.noisyaggregation.sketch;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.concurrent.NotThreadSafe;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * SfmSketch is a sketch for distinct counting, very similar to HyperLogLog.
 * This sketch is introduced as the Sketch-Flip-Merge (SFM) summary in the paper
 * <a href="https://arxiv.org/pdf/2302.02056.pdf">Sketch-Flip-Merge: Mergeable Sketches for Private Distinct Counting</a>.
 * <p>
 * The primary differences between SfmSketch and HyperLogLog are that
 * (a) SfmSketch supports differential privacy, and
 * (b) where HyperLogLog tracks only max observed bucket values, SfmSketch tracks all bucket values observed.
 * <p>
 * This means that SfmSketch is a larger sketch than HyperLogLog, but offers the ability to store completely
 * DP sketches with a fixed, public hash function while maintaining accurate cardinality estimates.
 * <p>
 * SfmSketch is created in a non-private mode. Privacy must be enabled through the enablePrivacy() function.
 * Once made private, the sketch becomes immutable. Privacy is quantified by the parameter epsilon.
 * <p>
 * When epsilon > 0, the sketch is epsilon-DP, and bits are randomized to preserve privacy.
 * When epsilon == NON_PRIVATE_EPSILON, the sketch is not private, and bits are set deterministically.
 * <p>
 * The best accuracy comes with NON_PRIVATE_EPSILON. For private epsilons, larger gives more accuracy,
 * while smaller gives more privacy.
 */
@NotThreadSafe
public class SfmSketch
{
    public static final double NON_PRIVATE_EPSILON = Double.POSITIVE_INFINITY;

    private static final byte FORMAT_TAG = 7;
    private static final int MAX_ESTIMATION_ITERATIONS = 1000;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SfmSketch.class).instanceSize();

    private final int indexBitLength;
    private final int precision;

    private double randomizedResponseProbability;
    private final Bitmap bitmap;

    private SfmSketch(Bitmap bitmap, int indexBitLength, int precision, double randomizedResponseProbability)
    {
        requireNonNull(bitmap, "bitmap cannot be null");
        validatePrefixLength(indexBitLength);
        validatePrecision(precision, indexBitLength);
        validateRandomizedResponseProbability(randomizedResponseProbability);

        this.bitmap = bitmap;
        this.indexBitLength = indexBitLength;
        this.precision = precision;
        this.randomizedResponseProbability = randomizedResponseProbability;
    }

    /**
     * Create a new SfmSketch in non-private mode. To make private,
     * call enablePrivacy() after populating the sketch.
     */
    public static SfmSketch create(int numberOfBuckets, int precision)
    {
        // Only create non-private sketches.
        // Private sketches are immutable, so they're kind of useless to create.
        double randomizedResponseProbability = getRandomizedResponseProbability(NON_PRIVATE_EPSILON);
        int indexBitLength = indexBitLength(numberOfBuckets);
        Bitmap bitmap = new Bitmap(numberOfBuckets * precision);
        return new SfmSketch(bitmap, indexBitLength, precision, randomizedResponseProbability);
    }

    public static SfmSketch deserialize(Slice serialized)
    {
        // Format:
        // format | indexBitLength | precision | epsilon | bitmap
        BasicSliceInput input = serialized.getInput();
        byte format = input.readByte();
        checkArgument(format == FORMAT_TAG, "Wrong format tag");

        int indexBitLength = input.readInt();
        int precision = input.readInt();
        double randomizedResponseProbability = input.readDouble();
        int bitmapByteLength = input.readInt();

        Bitmap bitmap = Bitmap.fromSliceInput(input, bitmapByteLength, numberOfBuckets(indexBitLength) * precision);
        return new SfmSketch(bitmap, indexBitLength, precision, randomizedResponseProbability);
    }

    public void add(long value)
    {
        addHash(Murmur3Hash128.hash64(value));
    }

    public void add(Slice value)
    {
        addHash(Murmur3Hash128.hash64(value));
    }

    public void addHash(long hash)
    {
        int index = computeIndex(hash, indexBitLength);
        // cap zeros at precision - 1
        // essentially, we're looking at a (precision - 1)-bit hash
        int zeros = Math.min(precision - 1, numberOfTrailingZeros(hash, indexBitLength));
        flipBitOn(index, zeros);
    }

    /**
     * Estimates cardinality via maximum psuedolikelihood (Newton's method)
     */
    public long cardinality()
    {
        // The initial guess of 1 may seem awful, but this converges quickly, and starting small returns better results for small cardinalities.
        // This generally takes <= 40 iterations, even for cardinalities as large as 10^33.
        double guess = 1;
        double changeInGuess = Double.POSITIVE_INFINITY;
        int iterations = 0;
        while (Math.abs(changeInGuess) > 0.1 && iterations < MAX_ESTIMATION_ITERATIONS) {
            changeInGuess = -logLikelihoodFirstDerivative(guess) / logLikelihoodSecondDerivative(guess);
            guess += changeInGuess;
            iterations += 1;
        }
        return Math.max(0, Math.round(guess));
    }

    public static int computeIndex(long hash, int indexBitLength)
    {
        return (int) (hash >>> (Long.SIZE - indexBitLength));
    }

    /**
     * Enable privacy on a non-privacy-enabled sketch
     * <p>
     * Per Lemma 4.7, <a href="https://arxiv.org/pdf/2302.02056.pdf">arXiv:2302.02056</a>,
     * flipping every bit with probability 1/(e^epsilon + 1) achieves differential privacy.
     */
    public void enablePrivacy(double epsilon)
    {
        enablePrivacy(epsilon, getDefaultRandomizationStrategy());
    }

    public void enablePrivacy(double epsilon, RandomizationStrategy randomizationStrategy)
    {
        requireNonNull(randomizationStrategy, "randomizationStrategy cannot be null");
        checkArgument(!isPrivacyEnabled(), "sketch is already privacy-enabled");
        validateEpsilon(epsilon);

        randomizedResponseProbability = getRandomizedResponseProbability(epsilon);

        // Flip every bit with fixed probability
        bitmap.flipAll(randomizedResponseProbability, randomizationStrategy);
    }

    public int estimatedSerializedSize()
    {
        return SizeOf.SIZE_OF_BYTE + // type + version
                SizeOf.SIZE_OF_INT + // indexBitLength
                SizeOf.SIZE_OF_INT + // precision
                SizeOf.SIZE_OF_DOUBLE + // randomized response probability
                SizeOf.SIZE_OF_INT + // bitmap byte length
                (bitmap.byteLength() * SizeOf.SIZE_OF_BYTE); // bitmap
    }

    private void flipBitOn(int bucket, int level)
    {
        checkArgument(!isPrivacyEnabled(), "privacy-enabled SfmSketch is immutable");

        int i = getBitLocation(bucket, level);
        bitmap.setBit(i, true);
    }

    @VisibleForTesting
    int getBitLocation(int bucket, int level)
    {
        return level * numberOfBuckets(indexBitLength) + bucket;
    }

    public Bitmap getBitmap()
    {
        return bitmap;
    }

    private static RandomizationStrategy getDefaultRandomizationStrategy()
    {
        return new SecureRandomizationStrategy();
    }

    @VisibleForTesting
    double getOnProbability()
    {
        // probability of a 1-bit remaining a 1-bit under randomized response
        return 1 - randomizedResponseProbability;
    }

    static double getRandomizedResponseProbability(double epsilon)
    {
        // If non-private, we don't use randomized response.
        // Otherwise, flip bits with probability 1/(exp(epsilon) + 1).
        if (epsilon == NON_PRIVATE_EPSILON) {
            return 0;
        }
        return 1.0 / (Math.exp(epsilon) + 1);
    }

    @VisibleForTesting
    double getRandomizedResponseProbability()
    {
        // probability of a 0-bit flipping to a 1-bit under randomized response
        return randomizedResponseProbability;
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + bitmap.getRetainedSizeInBytes();
    }

    public static int indexBitLength(int numberOfBuckets)
    {
        Preconditions.checkArgument(isPowerOf2(numberOfBuckets), "numberOfBuckets must be a power of 2, actual: %s", numberOfBuckets);
        // 2**N has N trailing zeros, and we've asserted numberOfBuckets == 2**N
        return Integer.numberOfTrailingZeros(numberOfBuckets);
    }

    public static boolean isPowerOf2(long value)
    {
        Preconditions.checkArgument(value > 0, "value must be positive");
        return (value & (value - 1)) == 0;
    }

    public boolean isPrivacyEnabled()
    {
        return getRandomizedResponseProbability() > 0;
    }

    private double logLikelihoodFirstDerivative(double n)
    {
        // Technically, this is the first derivative of the log of a psuedolikelihood.
        double result = 0;
        for (int level = 0; level < precision; level++) {
            double termOn = logLikelihoodTermFirstDerivative(level, true, n);
            double termOff = logLikelihoodTermFirstDerivative(level, false, n);
            for (int bucket = 0; bucket < numberOfBuckets(indexBitLength); bucket++) {
                result += bitmap.getBit(getBitLocation(bucket, level)) ? termOn : termOff;
            }
        }
        return result;
    }

    private double logLikelihoodTermFirstDerivative(int level, boolean on, double n)
    {
        double p = observationProbability(level);
        int sign = on ? -1 : 1;
        double c1 = on ? getOnProbability() : 1 - getOnProbability();
        double c2 = getOnProbability() - getRandomizedResponseProbability();
        return Math.log1p(-p) * (1 - c1 / (c1 + sign * c2 * Math.pow(1 - p, n)));
    }

    private double logLikelihoodSecondDerivative(double n)
    {
        // Technically, this is the second derivative of the log of a psuedolikelihood.
        double result = 0;
        for (int level = 0; level < precision; level++) {
            double termOn = logLikelihoodTermSecondDerivative(level, true, n);
            double termOff = logLikelihoodTermSecondDerivative(level, false, n);
            for (int bucket = 0; bucket < numberOfBuckets(indexBitLength); bucket++) {
                result += bitmap.getBit(getBitLocation(bucket, level)) ? termOn : termOff;
            }
        }
        return result;
    }

    private double logLikelihoodTermSecondDerivative(int level, boolean on, double n)
    {
        double p = observationProbability(level);
        int sign = on ? -1 : 1;
        double c1 = on ? getOnProbability() : 1 - getOnProbability();
        double c2 = getOnProbability() - getRandomizedResponseProbability();
        return sign * c1 * c2 * Math.pow(Math.log1p(-p), 2) * Math.pow(1 - p, n) * Math.pow(c1 + sign * c2 * Math.pow(1 - p, n), -2);
    }

    /**
     * Merging two sketches with randomizedResponseProbability values p1 and p2 is equivalent to
     * having created two non-private sketches, merged them, then enabled privacy with a
     * randomizedResponseProbability value of:
     * <p>
     * (p1 + p2 - 3 * p1 * p2) / (1 - 2 * p1 * p2)
     * <p>
     * This can be derived from the fact that two private sketches created with epsilon1 and epsilon2
     * merge to be equivalent to a single sketch created with epsilon:
     * <p>
     * -log(exp(-epsilon1) + exp(-epsilon2) - exp(-(epsilon1 + epsilon2))
     * <p>
     * For details, see Theorem 4.8, <a href="https://arxiv.org/pdf/2302.02056.pdf">arXiv:2302.02056</a>.
     * For verification, see the unit tests.
     */
    @VisibleForTesting
    static double mergeRandomizedResponseProbabilities(double p1, double p2)
    {
        return (p1 + p2 - 3 * p1 * p2) / (1 - 2 * p1 * p2);
    }

    /**
     * Performs a merge of the other sketch into the current sketch. This is performed
     * as a randomized merge as described in Theorem 4.8,
     * <a href="https://arxiv.org/pdf/2302.02056.pdf">arXiv:2302.02056</a>.
     * <p>
     * The formula used in this function is a simplification of the form presented in the original paper.
     * See also Section 3, <a href="https://arxiv.org/pdf/2306.09394.pdf">arXiv:2306.09394</a>.
     */
    public void mergeWith(SfmSketch other)
    {
        mergeWith(other, getDefaultRandomizationStrategy());
    }

    public void mergeWith(SfmSketch other, RandomizationStrategy randomizationStrategy)
    {
        requireNonNull(randomizationStrategy, "randomizationStrategy cannot be null");

        // Strictly speaking, we may be able to provide more general merging than suggested here.
        // It's not clear how useful this would be in practice.
        checkArgument(precision == other.precision, "cannot merge two SFM sketches with different precision: %s vs. %s", precision, other.precision);
        checkArgument(indexBitLength == other.indexBitLength, "cannot merge two SFM sketches with different indexBitLength: %s vs. %s",
                indexBitLength, other.indexBitLength);

        if (!isPrivacyEnabled() && !other.isPrivacyEnabled()) {
            // if neither sketch is private, we just take the OR of the sketches
            bitmap.or(other.getBitmap());
        }
        else {
            // if either sketch is private, we combine using a randomized merge
            // (the non-private case above is a special case of this more complicated math)
            double p1 = randomizedResponseProbability;
            double p2 = other.randomizedResponseProbability;
            double p = mergeRandomizedResponseProbabilities(p1, p2);
            double normalizer = (1 - 2 * p) / ((1 - 2 * p1) * (1 - 2 * p2));

            for (int i = 0; i < bitmap.length(); i++) {
                double bit1 = bitmap.getBit(i) ? 1 : 0;
                double bit2 = other.bitmap.getBit(i) ? 1 : 0;
                double x = 1 - 2 * p - normalizer * (1 - p1 - bit1) * (1 - p2 - bit2);
                double probability = p + normalizer * x;
                probability = Math.min(1.0, Math.max(0.0, probability));
                bitmap.setBit(i, randomizationStrategy.nextBoolean(probability));
            }
        }

        randomizedResponseProbability = mergeRandomizedResponseProbabilities(randomizedResponseProbability, other.randomizedResponseProbability);
    }

    public static int numberOfBuckets(int indexBitLength)
    {
        return 1 << indexBitLength;
    }

    public static int numberOfTrailingZeros(long hash, int indexBitLength)
    {
        long value = hash | (1L << (Long.SIZE - indexBitLength)); // place a 1 in the final position of the prefix to avoid flowing into prefix when the hash happens to be 0
        return Long.numberOfTrailingZeros(value);
    }

    private double observationProbability(int level)
    {
        // probability of observing a run of zeros of length level in any single bucket
        // note: this is NOT (in general) the probability of having a 1 in the corresponding location in the sketch
        // (it is if bits are set deterministically, as when epsilon < 0)
        return Math.pow(2.0, -(level + 1)) / numberOfBuckets(indexBitLength);
    }

    public Slice serialize()
    {
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(estimatedSerializedSize());
        serialize(sliceOutput);
        return sliceOutput.slice();
    }

    public void serialize(DynamicSliceOutput sliceOutput)
    {
        byte[] bitmapBytes = bitmap.toBytes();
        sliceOutput.appendByte(FORMAT_TAG)
                .appendInt(indexBitLength)
                .appendInt(precision)
                .appendDouble(randomizedResponseProbability)
                .appendInt(bitmapBytes.length)
                .appendBytes(bitmapBytes);
    }

    private static void validateEpsilon(double epsilon)
    {
        checkArgument(epsilon > 0, "epsilon must be greater than zero or equal to NON_PRIVATE_EPSILON");
    }

    private static void validatePrecision(int precision, int indexBitLength)
    {
        checkArgument(precision > 0, "precision must be positive", Byte.SIZE);
        checkArgument(precision + indexBitLength <= Long.SIZE, "precision + indexBitLength cannot exceed %s", Long.SIZE);
    }

    private static void validatePrefixLength(int indexBitLength)
    {
        checkArgument(indexBitLength >= 1 && indexBitLength <= 32, "indexBitLength is out of range");
    }

    private static void validateRandomizedResponseProbability(double p)
    {
        checkArgument(p >= 0 && p <= 0.5, "randomizedResponseProbability should be in the interval [0, 0.5]");
    }
}
