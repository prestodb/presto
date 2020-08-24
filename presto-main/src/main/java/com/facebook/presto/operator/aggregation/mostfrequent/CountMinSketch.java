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

package com.facebook.presto.operator.aggregation.mostfrequent;

import com.facebook.presto.spi.PrestoException;
import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Murmur3Hash32;
import io.airlift.slice.SizeOf;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;
import java.util.Random;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_INT;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;

/**
 * Copied from: https://github.com/addthis/stream-lib
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CountMinSketch
{
    public static final long PRIME_MODULUS = (1L << 31) - 1;
    private static final long serialVersionUID = -5084982213094657923L;
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(CountMinSketch.class).instanceSize();

    int depth;
    int width;
    long[][] table;
    long[] hashA;
    long size;
    long estimatedInMemorySize;
    double eps;
    double confidence;

    /**
     * This class is to find heavy hitters and based upon the paper: http://theory.stanford.edu/~tim/s17/l/l2.pdf
     *
     * @param epsOfTotalCount error bound such that counts are overestimated by at most epsError X n. Default value=1/2k
     * @param confidence probability that the count is overestimated by more than the error bound(epsError*n). Default value=0.01
     * @param seed
     */
    public CountMinSketch(double epsOfTotalCount, double confidence, int seed)
    {
        if (epsOfTotalCount <= 0 || epsOfTotalCount >= 1 || confidence <= 0 || confidence >= 1) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "epsOfTotalCount and confidence should be greater than 0 and less than 1");
        }
        // e/w = eps ; w = e/eps  Where e is the natural log base.
        // 1/2^depth <= 1-confidence ; depth >= log2(1/(1-confidence))
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(Math.E / epsOfTotalCount);
        this.depth = (int) Math.ceil(Math.log(1 / (1 - confidence)));
        initTablesWith(depth, width, seed);
    }

    @Override
    public String toString()
    {
        return "CountMinSketch{" +
                "eps=" + eps +
                ", confidence=" + confidence +
                ", depth=" + depth +
                ", width=" + width +
                ", size=" + size +
                '}';
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

        final CountMinSketch that = (CountMinSketch) o;

        if (depth != that.depth) {
            return false;
        }
        if (width != that.width) {
            return false;
        }

        if (Double.compare(that.eps, eps) != 0) {
            return false;
        }
        if (Double.compare(that.confidence, confidence) != 0) {
            return false;
        }

        if (size != that.size) {
            return false;
        }

        if (!Arrays.deepEquals(table, that.table)) {
            return false;
        }
        return Arrays.equals(hashA, that.hashA);
    }

    @Override
    public int hashCode()
    {
        int result;
        long temp;
        result = depth;
        result = 31 * result + width;
        result = 31 * result + Arrays.deepHashCode(table);
        result = 31 * result + Arrays.hashCode(hashA);
        result = 31 * result + (int) (size ^ (size >>> 32));
        temp = Double.doubleToLongBits(eps);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(confidence);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }

    private void initTablesWith(int depth, int width, int seed)
    {
        this.table = new long[depth][width];
        this.hashA = new long[depth];
        Random r = new Random(seed);
        // We're using a linear hash functions
        // of the form (a*x+b) mod p.
        // a,b are chosen independently for each hash function.
        // However we can set b = 0 as all it does is shift the results
        // without compromising their uniformity or independence with
        // the other hashes.
        for (int i = 0; i < depth; ++i) {
            hashA[i] = r.nextInt(Integer.MAX_VALUE);
        }
        estimatedInMemorySize += SIZE_OF_LONG * (depth * width + depth) + SizeOf.sizeOf(hashA) + width * SizeOf.sizeOf(table);
    }

    public double getRelativeError()
    {
        return eps;
    }

    public double getConfidence()
    {
        return confidence;
    }

    private static void checkSizeAfterOperation(long previousSize, String operation, long newSize)
    {
        if (newSize < previousSize) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Overflow error: max number of rows allowed is equal to the Long.MAX");
        }
    }

    private void checkSizeAfterAdd(long count)
    {
        long previousSize = size;
        size += count;
        checkSizeAfterOperation(previousSize, "add", size);
    }

    public long add(Slice item, long count)
    {
        return add(getHashBuckets(item, depth, width), count);
    }

    public long add(int[] buckets, long count)
    {
        if (count < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Negative increments not implemented");
        }

        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
            res = Math.min(res, table[i][buckets[i]]);
        }
        checkSizeAfterAdd(count);
        return res;
    }

    public long size()
    {
        return size;
    }

    public long estimateCount(Slice item)
    {
        return estimateCount(getHashBuckets(item, depth, width));
    }

    public long estimateCount(int[] buckets)
    {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    // Murmur is faster than an SHA-based approach and provides as-good collision
    // resistance.  The combinatorial generation approach described in
    // https://gnunet.org/sites/default/files/LessHashing2006Kirsch.pdf
    // does prove to work in actual tests, and is obviously faster
    // than performing further iterations of murmur.
    static int[] getHashBuckets(Slice item, int hashCount, int max)
    {
        int[] result = new int[hashCount];
        int hash1 = Murmur3Hash32.hash(0, item, 0, item.length());
        int hash2 = Murmur3Hash32.hash(hash1, item, 0, item.length());
        for (int i = 0; i < hashCount; i++) {
            result[i] = Math.abs((hash1 + i * hash2) % max);
        }
        return result;
    }

    public CountMinSketch merge(CountMinSketch... cmss)
            throws IllegalArgumentException
    {
        for (CountMinSketch cms : cmss) {
            if (cms.depth != depth || cms.width != width || !Arrays.equals(cms.hashA, hashA)) {
                throw new IllegalArgumentException("Cannot merge CountMinSketch of different shapes(depth, width, hash)" + "this:(" + depth + "," + width + "," + hashA.toString() + ") " + " other:(" + cms.depth + "," + cms.width + "," + cms.hashA.toString() + ")");
            }

            for (int i = 0; i < table.length; i++) {
                for (int j = 0; j < table[i].length; j++) {
                    table[i][j] += cms.table[i][j];
                }
            }
            long previousSize = size;
            size += cms.size;
            checkSizeAfterOperation(previousSize, "merge", size);
        }

        return this;
    }

    public Slice serialize()
    {
        int requiredBytes =
                2 * SIZE_OF_INT +    //depth, width
                        2 * SIZE_OF_LONG + //size, estimatedInMemorySize
                        depth * SIZE_OF_LONG + // long[] hashA
                        depth * width * SIZE_OF_LONG + // long[][] table
                        2 * SIZE_OF_DOUBLE; //eps, confidence

        SliceOutput s = new DynamicSliceOutput(requiredBytes);
        s.writeLong(size);
        s.writeInt(depth);
        s.writeInt(width);
        s.writeLong(estimatedInMemorySize);
        s.writeDouble(eps);
        s.writeDouble(confidence);
        for (int i = 0; i < depth; ++i) {
            s.writeLong(hashA[i]);
            for (int j = 0; j < width; ++j) {
                s.writeLong(table[i][j]);
            }
        }
        return s.slice();
    }

    //Constructor based upon deserialization
    public CountMinSketch(Slice serialized)
    {
        SliceInput s = new BasicSliceInput(serialized);
        size = s.readLong();
        depth = s.readInt();
        width = s.readInt();
        estimatedInMemorySize = s.readLong();
        eps = s.readDouble();
        confidence = s.readDouble();
        hashA = new long[depth];
        table = new long[depth][width];
        for (int i = 0; i < depth; ++i) {
            hashA[i] = s.readLong();
            for (int j = 0; j < width; ++j) {
                table[i][j] = s.readLong();
            }
        }
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + estimatedInMemorySize;
    }
}
