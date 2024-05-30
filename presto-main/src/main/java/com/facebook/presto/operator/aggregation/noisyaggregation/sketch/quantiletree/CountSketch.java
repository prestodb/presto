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
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Murmur3Hash128;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.SizeOf.SIZE_OF_FLOAT;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;

/**
 * An approximate multiset structure, allowing for compact approximation of counts of longs
 */
public class CountSketch
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(CountSketch.class).instanceSize();
    private static final long PRIME_FIELD_SIZE = (1L << 31) - 1;

    private final int depth;
    private final int width;

    private final float[][] table;

    /**
     * Create a new CountSketch with fixed dimensions
     */
    public CountSketch(int depth, int width)
    {
        if (depth <= 0 || width <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "sketch dimensions (depth and width) must be positive");
        }

        this.width = width;
        this.depth = depth;
        this.table = new float[depth][width];
    }

    /**
     * Deserialize an existing CountSketch
     */
    public CountSketch(SliceInput s)
    {
        depth = s.readInt();
        width = s.readInt();
        table = new float[depth][width];
        for (int i = 0; i < depth; ++i) {
            for (int j = 0; j < width; ++j) {
                table[i][j] = s.readFloat();
            }
        }
    }

    /**
     * Adds noise to the counters (table) to enable privacy
     * @param rho zCDP privacy budget
     * @param insertionCount number of items potentially inserted into the sketch between neighbors
     */
    public void addNoise(double rho, int insertionCount, RandomizationStrategy randomizationStrategy)
    {
        if (rho <= 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "rho must be greater than zero");
        }

        // Assuming unbounded neighbors, the squared L2 sensitivity is depth * insertionCount^2,
        // as (without loss of generality) the worst-case object adds +1 to exactly D = depth
        // buckets I = insertionCount times, yielding a difference between neighbors of:
        // I, 0, 0, 0...
        // I, 0, 0, 0...
        // ...
        // (repeated D times)
        //
        // To achieve rho-zCDP, we add noise with variance depth * insertionCount^2 / (2 * rho),
        // i.e., with standard deviation insertionCount * sqrt(0.5 * depth / rho).
        for (int i = 0; i < depth; i++) {
            for (int j = 0; j < width; j++) {
                table[i][j] += randomizationStrategy.nextGaussian() * insertionCount * Math.sqrt(0.5 * depth / rho);
            }
        }
    }

    public int getDepth()
    {
        return depth;
    }

    public int getWidth()
    {
        return width;
    }

    public void add(long item, float count)
    {
        long baseHash = getBaseHash(item);
        for (int i = 0; i < depth; ++i) {
            long iteratedHash = getIteratedHash(baseHash, i);
            table[i][getBucket(iteratedHash)] += count * getSign(iteratedHash);
        }
    }

    public void add(long item)
    {
        add(item, 1);
    }

    public long estimateCount(long item)
    {
        long baseHash = getBaseHash(item);
        float[] values = new float[depth];
        for (int i = 0; i < depth; ++i) {
            long iteratedHash = getIteratedHash(baseHash, i);
            values[i] = getSign(iteratedHash) * table[i][getBucket(iteratedHash)];
        }
        Arrays.sort(values);
        int mid = Math.floorDiv(depth, 2);
        return Math.round(values[mid]);
    }

    /**
     * A 64-bit hash used to derive further hashes (see getIteratedHash)
     */
    @VisibleForTesting
    static long getBaseHash(long item)
    {
        return Murmur3Hash128.hash64(item);
    }

    /**
     * Following the style of Kirsch and Mitzenmacher's "Less Hashing, Same Performance"
     * (https://www.eecs.harvard.edu/~michaelm/postscripts/rsa2008.pdf),
     * we compute multiple hashes for the sketch by mutating two base sketches, h1 and h2,
     * which we take to be the two halves of a 64-bit base sketch.
     * @returns a new hash derived from baseHash over the field of size 2 * width
     */
    @VisibleForTesting
    long getIteratedHash(long baseHash, int i)
    {
        // Partition baseHash into left and right halves
        long h1 = baseHash >> 32;
        long h2 = baseHash & Integer.MAX_VALUE;

        // Derive a new hash over a prime field
        long primeHash = Math.abs(h1 + i * h2) % PRIME_FIELD_SIZE;

        // Return a hash in the field of size 2 * width
        return primeHash % (2 * width);
    }

    /**
     * Calculate the bucket index from an iterated hash.
     * Since the iteratedHash is modulo 2 * width, we can drop the least significant bit (used for sign) to
     * obtain a bucket index modulo width.
     */
    @VisibleForTesting
    static int getBucket(long iteratedHash)
    {
        return (int) (iteratedHash >> 1);
    }

    /**
     * Calculate the sign from an iterated hash. We use the least significant bit to determine sign, and leave
     * the rest of the hash to calculate bucket.
     */
    @VisibleForTesting
    static int getSign(long iteratedHash)
    {
        long sign = iteratedHash & 1L;
        return sign == 1 ? 1 : -1;
    }

    public CountSketch merge(CountSketch... otherSketches)
            throws IllegalArgumentException
    {
        for (CountSketch other : otherSketches) {
            if (other.depth != depth || other.width != width) {
                throw new IllegalArgumentException("Cannot merge CountSketch of different shapes (depth, width):" +
                        "(" + depth + "," + width + ") vs. (" + other.depth + "," + other.width + ")");
            }

            for (int i = 0; i < table.length; i++) {
                for (int j = 0; j < table[i].length; j++) {
                    table[i][j] += other.table[i][j];
                }
            }
        }

        return this;
    }

    public int estimatedSerializedSizeInBytes()
    {
        return 2 * SIZE_OF_INT +    //depth, width
                depth * width * SIZE_OF_FLOAT; // float[][] table
    }

    public Slice serialize()
    {
        int requiredBytes = estimatedSerializedSizeInBytes();

        SliceOutput s = new DynamicSliceOutput(requiredBytes);
        s.writeInt(depth);
        s.writeInt(width);
        for (int i = 0; i < depth; ++i) {
            for (int j = 0; j < width; ++j) {
                s.writeFloat(table[i][j]);
            }
        }
        return s.slice();
    }

    public long estimatedSizeInBytes()
    {
        return INSTANCE_SIZE +
                SizeOf.sizeOf(table) + depth * SizeOf.sizeOfFloatArray(width); // float[depth][width] table
    }
}
