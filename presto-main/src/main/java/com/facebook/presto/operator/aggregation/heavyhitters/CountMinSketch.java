/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.facebook.presto.operator.aggregation.heavyhitters;

import io.airlift.slice.*;
import org.openjdk.jol.info.ClassLayout;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import java.util.Arrays;
import java.util.Random;

import static io.airlift.slice.SizeOf.*;


/**
 * Copied from: https://github.com/addthis/stream-lib
 * Count-Min Sketch datastructure.
 * An Improved Data Stream Summary: The Count-Min Sketch and its Applications
 * https://web.archive.org/web/20060907232042/http://www.eecs.harvard.edu/~michaelm/CS222/countmin.pdf
 */
public class CountMinSketch {

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


    CountMinSketch() {
    }

//TODO this method is not required and should be deleted
//    public CountMinSketch(int depth, int width, int seed) {
//        this.depth = depth;
//        this.width = width;
//        this.eps = 2.0 / width;
//        this.confidence = 1 - 1 / Math.pow(2, depth);
//        initTablesWith(depth, width, seed);
//    }

    /**
     * This class is to find heavy hitters and based upon the paper: http://theory.stanford.edu/~tim/s17/l/l2.pdf
     * @param epsOfTotalCount error bound such that counts are overestimated by at most epsError X n. Default value=1/2k
     * @param confidence probability that the count is overestimated by more than the error bound(epsError*n). Default value=0.01
     * @param seed
     */
    public CountMinSketch(double epsOfTotalCount, double confidence, int seed) {

        if(epsOfTotalCount <= 0 || epsOfTotalCount >=1 || confidence <=0 || confidence >=1){
            throw new IllegalArgumentException("epsOfTotalCount and confidence should be greater than 0 and less than 1");
        }
        // e/w = eps ; w = e/eps  Where e is the natural log base.
        // 1/2^depth <= 1-confidence ; depth >= log2(1/(1-confidence))
        this.eps = epsOfTotalCount;
        this.confidence = confidence;
        this.width = (int) Math.ceil(Math.E / epsOfTotalCount);
        this.depth = (int) Math.ceil(Math.log(1/(1-confidence)));
        initTablesWith(depth, width, seed);
        //TODO: Is this efficient way to calculate size since the size is constant here?
        estimatedInMemorySize=SizeOf.sizeOf(table) + SizeOf.sizeOf(hashA);
    }

//    CountMinSketch(int depth, int width, long size, long[] hashA, long[][] table) {
//        this.depth = depth;
//        this.width = width;
//        this.eps = 2.0 / width;
//        this.confidence = 1 - 1 / Math.pow(2, depth);
//        this.hashA = hashA;
//        this.table = table;
//
//        if(size < 0){
//            throw new IllegalArgumentException("The size cannot be smaller than ZER0: " + size);
//        }
//        this.size = size;
//    }

    @Override
    public String toString() {
        return "CountMinSketch{" +
                "eps=" + eps +
                ", confidence=" + confidence +
                ", depth=" + depth +
                ", width=" + width +
                ", size=" + size +
                '}';
    }

    @Override
    public boolean equals(Object o) {
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
    public int hashCode() {
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

    private void initTablesWith(int depth, int width, int seed) {
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
    }

    public double getRelativeError() {
        return eps;
    }

    public double getConfidence() {
        return confidence;
    }

    int hash(long item, int i) {
        long hash = hashA[i] * item;
        // A super fast way of computing x mod 2^p-1
        // See http://www.cs.princeton.edu/courses/archive/fall09/cos521/Handouts/universalclasses.pdf
        // page 149, right after Proposition 7.
        hash += hash >> 32;
        hash &= PRIME_MODULUS;
        // Doing "%" after (int) conversion is ~2x faster than %'ing longs.
        return ((int) hash) % width;
    }

    private static void checkSizeAfterOperation(long previousSize, String operation, long newSize) {
        if (newSize < previousSize) {
            throw new IllegalStateException("Overflow error: the size after calling `" + operation +
                    "` is smaller than the previous size. " +
                    "Previous size: " + previousSize +
                    ", New size: " + newSize);
        }
    }

    private void checkSizeAfterAdd(String item, long count) {
        long previousSize = size;
        size += count;
        checkSizeAfterOperation(previousSize, "add(" + item + "," + count + ")", size);
    }

    /**
     *
     * @param item
     * @param count
     * @return the latest estimateCount after adding the element to avoid having to immediately call estimateCount if required.
     */
    public long add(long item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }

        long res = Long.MAX_VALUE;
        int j;
        for (int i = 0; i < depth; ++i) {
            j=hash(item, i);
            table[i][j] += count;
            res = Math.min(res, table[i][j]);
        }
        checkSizeAfterAdd(String.valueOf(item), count);
        return res;
    }

    /**
     *
     * @param item
     * @param count
     * @return the latest estimateCount after adding the element to avoid having to immediately call estimateCount if required.
     */
    public long add(String item, long count) {
        if (count < 0) {
            // Actually for negative increments we'll need to use the median
            // instead of minimum, and accuracy will suffer somewhat.
            // Probably makes sense to add an "allow negative increments"
            // parameter to constructor.
            throw new IllegalArgumentException("Negative increments not implemented");
        }

        long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            table[i][buckets[i]] += count;
            res = Math.min(res, table[i][buckets[i]]);
        }
        checkSizeAfterAdd(item, count);
        return res;
    }

    public long size() {
        return size;
    }

    /**
     * The estimate is correct within 'epsilon' * (total item count),
     * with probability 'confidence'.
     */
    public long estimateCount(long item) {
        long res = Long.MAX_VALUE;
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][hash(item, i)]);
        }
        return res;
    }

    public long estimateCount(String item) {
        long res = Long.MAX_VALUE;
        int[] buckets = Filter.getHashBuckets(item, depth, width);
        for (int i = 0; i < depth; ++i) {
            res = Math.min(res, table[i][buckets[i]]);
        }
        return res;
    }

    /**
     * Merges count min sketches to produce a count min sketch for their combined streams
     *
     * @param cmss list of CountMinSketch to be merged into this
     * @return merged estimator or null if no estimators were provided
     * @throws CMSMergeException if estimators are not mergeable (same depth, width and seed)
     */
    public CountMinSketch merge(CountMinSketch... cmss) throws CMSMergeException {
        for(CountMinSketch cms: cmss) {
            if (cms != null) {
                if (cms.depth != depth || cms.width != width || !Arrays.equals(cms.hashA, hashA)) {
                    throw new CMSMergeException("Cannot merge CountMinSketch of different shapes(depth, width, hash)" + "this:(" + depth + "," + width + "," + hashA.toString() + ") " + " other:(" + cms.depth + "," + cms.width + "," + cms.hashA.toString() + ")");
                }

                for (int i = 0; i < table.length; i++) {
                    for (int j = 0; j < table[i].length; j++) {
                        table[i][j] += cms.table[i][j];
                    }
                }

                long previousSize = size;
                size += cms.size;
                checkSizeAfterOperation(previousSize, "merge(" + cms + ")", size);
            }
        }
        return this;
    }

    public Slice serialize() {
        SliceOutput s = new DynamicSliceOutput(estimatedSerializedSizeInBytes());
        s.writeLong(size);
        s.writeInt(depth);
        s.writeInt(width);
        s.writeLong(size);
        s.writeLong(estimatedInMemorySize);
        for (int i = 0; i < depth; ++i) {
            s.writeLong(hashA[i]);
            for (int j = 0; j < width; ++j) {
                s.writeLong(table[i][j]);
            }
        }
        return s.slice();
    }


    //Constructor based upon deserialization
    public CountMinSketch(Slice serialized) {
        SliceInput s = new BasicSliceInput(serialized);
        size = s.readLong();
        depth = s.readInt();
        width = s.readInt();
        size = s.readLong();
        estimatedInMemorySize = s.readLong();
        // e/w = eps ; w = e/eps  Where e is the natural log base.
        // 1/2^depth <= 1-confidence ; depth >= log2(1/(1-confidence))
        eps = Math.E / width;
        confidence = 1 - 1 / Math.pow(Math.E, depth);
        hashA = new long[depth];
        table = new long[depth][width];
        for (int i = 0; i < depth; ++i) {
            hashA[i] = s.readLong();
            for (int j = 0; j < width; ++j) {
                table[i][j] = s.readLong();
            }
        }
    }


    @SuppressWarnings("serial")
    protected static class CMSMergeException extends Exception {

        public CMSMergeException(String message) {
            super(message);
        }
    }


    public int estimatedSerializedSizeInBytes() {
        //TODO is this estimation correct
//        int depth;
//        int width;
//        long[][] table;
//        long[] hashA;
//        long size;
//        long estimatedInMemorySize;
//        double eps;
//        double confidence;

        return 2*SIZE_OF_INT + (2 + depth + depth*width)*SIZE_OF_LONG + 2*SIZE_OF_DOUBLE;
    }

    //TODO is this the best way to get size. Does this cover all the memory this data structure uses?
    public long estimatedInMemorySize() {
        return INSTANCE_SIZE + estimatedInMemorySize;
    }
}