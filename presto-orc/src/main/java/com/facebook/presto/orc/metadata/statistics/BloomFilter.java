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
package com.facebook.presto.orc.metadata.statistics;

import com.google.common.annotations.VisibleForTesting;

import java.util.Arrays;

// This class is Forked from class org.apache.hive.common.util.BloomFilter from the com.facebook.presto.hive:hive-apache
// https://github.com/apache/hive/blob/master/storage-api/src/java/org/apache/hive/common/util/BloomFilter.java
// hive-apache has org.apache.hadoop.hive.ql.exec.vector.*ColumnVector classes which conflicts with consumers of presto-orc
public class BloomFilter
{
    protected BloomFilter.BitSet bitSet;
    protected int numBits;
    protected int numHashFunctions;

    public BloomFilter()
    {
    }

    static void checkArgument(boolean expression, String message)
    {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
    }

    public BloomFilter(long expectedEntries, double fpp)
    {
        checkArgument(expectedEntries > 0L, "expectedEntries should be > 0");
        checkArgument(fpp > 0.0D && fpp < 1.0D, "False positive probability should be > 0.0 & < 1.0");
        int nb = optimalNumOfBits(expectedEntries, fpp);
        this.numBits = nb + (64 - nb % 64);
        this.numHashFunctions = optimalNumOfHashFunctions(expectedEntries, this.numBits);
        this.bitSet = new BitSet(this.numBits);
    }

    static int optimalNumOfHashFunctions(long n, long m)
    {
        return Math.max(1, (int) Math.round((double) m / (double) n * Math.log(2.0D)));
    }

    static int optimalNumOfBits(long n, double p)
    {
        return (int) ((double) (-n) * Math.log(p) / (Math.log(2.0D) * Math.log(2.0D)));
    }

    public void add(byte[] val)
    {
        if (val == null) {
            this.addBytes(val, -1, -1);
        }
        else {
            this.addBytes(val, 0, val.length);
        }
    }

    public void addBytes(byte[] val, int offset, int length)
    {
        long hash64 = val == null ? Murmur3.NULL_HASHCODE : Murmur3.hash64(val, offset, length);
        this.addHash(hash64);
    }

    private void addHash(long hash64)
    {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.numHashFunctions; ++i) {
            int combinedHash = hash1 + (i) * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }

            int pos = combinedHash % this.numBits;
            this.bitSet.set(pos);
        }
    }

    public void addString(String val)
    {
        if (val == null) {
            this.add(null);
        }
        else {
            this.add(val.getBytes());
        }
    }

    public void addLong(long val)
    {
        this.addHash(this.getLongHash(val));
    }

    public void addDouble(double val)
    {
        this.addLong(Double.doubleToLongBits(val));
    }

    public boolean test(byte[] val)
    {
        return val == null ? this.testBytes(val, -1, -1) : this.testBytes(val, 0, val.length);
    }

    public boolean testBytes(byte[] val, int offset, int length)
    {
        long hash64 = val == null ? Murmur3.NULL_HASHCODE : Murmur3.hash64(val, offset, length);
        return this.testHash(hash64);
    }

    private boolean testHash(long hash64)
    {
        int hash1 = (int) hash64;
        int hash2 = (int) (hash64 >>> 32);

        for (int i = 1; i <= this.numHashFunctions; ++i) {
            int combinedHash = hash1 + (i) * hash2;
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }

            int pos = combinedHash % this.numBits;
            if (!this.bitSet.get(pos)) {
                return false;
            }
        }

        return true;
    }

    public boolean testString(String val)
    {
        return val == null ? this.test(null) : this.test(val.getBytes());
    }

    public boolean testLong(long val)
    {
        return this.testHash(this.getLongHash(val));
    }

    private long getLongHash(long key)
    {
        key = ~key + (key << 21);
        key ^= key >> 24;
        key = key + (key << 3) + (key << 8);
        key ^= key >> 14;
        key = key + (key << 2) + (key << 4);
        key ^= key >> 28;
        key += key << 31;
        return key;
    }

    public boolean testDouble(double val)
    {
        return this.testLong(Double.doubleToLongBits(val));
    }

    public long sizeInBytes()
    {
        return this.getBitSize() / 8;
    }

    public int getBitSize()
    {
        return this.bitSet.getData().length * 64;
    }

    public int getNumHashFunctions()
    {
        return this.numHashFunctions;
    }

    public long[] getBitSet()
    {
        return this.bitSet.getData();
    }

    public String toString()
    {
        return "m: " + this.numBits + " k: " + this.numHashFunctions;
    }

    public void merge(BloomFilter that)
    {
        if (this != that && this.numBits == that.numBits && this.numHashFunctions == that.numHashFunctions) {
            this.bitSet.putAll(that.bitSet);
        }
        else {
            throw new IllegalArgumentException("BloomFilters are not compatible for merging. this - " + this.toString() + " that - " + that.toString());
        }
    }

    public void reset()
    {
        this.bitSet.clear();
    }

    public static class BitSet
    {
        private final long[] data;

        public BitSet(long bits)
        {
            this(new long[(int) Math.ceil((double) bits / 64.0D)]);
        }

        public BitSet(long[] data)
        {
            assert data.length > 0 : "data length is zero!";

            this.data = data;
        }

        public void set(int index)
        {
            long[] var10000 = this.data;
            var10000[index >>> 6] |= 1L << index;
        }

        public boolean get(int index)
        {
            return (this.data[index >>> 6] & 1L << index) != 0L;
        }

        public long bitSize()
        {
            return (long) this.data.length * 64L;
        }

        public long[] getData()
        {
            return this.data;
        }

        public void putAll(BloomFilter.BitSet array)
        {
            assert this.data.length == array.data.length : "BitArrays must be of equal length (" + this.data.length + "!= " + array.data.length + ")";

            for (int i = 0; i < this.data.length; ++i) {
                long[] var10000 = this.data;
                var10000[i] |= array.data[i];
            }
        }

        public void clear()
        {
            Arrays.fill(this.data, 0L);
        }
    }

    // Murmur3 code was originally from com.facebook.presto.hive:hive-apache jar in the class org.apache.hive.common.util.Murmur3
    // https://github.com/apache/hive/blob/master/storage-api/src/java/org/apache/hive/common/util/Murmur3.java
    // hive-apache has org.apache.hadoop.hive.ql.exec.vector.*ColumnVector classes which conflicts with consumers of presto-orc
    public static class Murmur3
    {
        public static final long NULL_HASHCODE = 2862933555777941757L;

        private static final long C1 = 0x87c37b91114253d5L;
        private static final long C2 = 0x4cf5ad432745937fL;
        private static final int R1 = 31;
        private static final int R2 = 27;
        private static final int M = 5;
        private static final int N1 = 0x52dce729;

        public static final int DEFAULT_SEED = 104729;

        /**
         * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
         *
         * @param data - input byte array
         * @return - hashcode
         */
        @VisibleForTesting
        static long hash64(byte[] data)
        {
            return hash64(data, 0, data.length);
        }

        public static long hash64(byte[] data, int offset, int length)
        {
            return hash64(data, offset, length, DEFAULT_SEED);
        }

        /**
         * Murmur3 64-bit variant. This is essentially MSB 8 bytes of Murmur3 128-bit variant.
         *
         * @param data - input byte array
         * @param length - length of array
         * @param seed - seed. (default is 0)
         * @return - hashcode
         */
        public static long hash64(byte[] data, int offset, int length, int seed)
        {
            long hash = seed;
            final int nblocks = length >> 3;

            // body
            for (int i = 0; i < nblocks; i++) {
                final int i8 = i << 3;
                long k = ((long) data[offset + i8] & 0xff)
                        | (((long) data[offset + i8 + 1] & 0xff) << 8)
                        | (((long) data[offset + i8 + 2] & 0xff) << 16)
                        | (((long) data[offset + i8 + 3] & 0xff) << 24)
                        | (((long) data[offset + i8 + 4] & 0xff) << 32)
                        | (((long) data[offset + i8 + 5] & 0xff) << 40)
                        | (((long) data[offset + i8 + 6] & 0xff) << 48)
                        | (((long) data[offset + i8 + 7] & 0xff) << 56);

                // mix functions
                k *= C1;
                k = Long.rotateLeft(k, R1);
                k *= C2;
                hash ^= k;
                hash = Long.rotateLeft(hash, R2) * M + N1;
            }

            // tail
            long k1 = 0;
            int tailStart = nblocks << 3;
            switch (length - tailStart) {
                case 7:
                    k1 ^= ((long) data[offset + tailStart + 6] & 0xff) << 48;
                case 6:
                    k1 ^= ((long) data[offset + tailStart + 5] & 0xff) << 40;
                case 5:
                    k1 ^= ((long) data[offset + tailStart + 4] & 0xff) << 32;
                case 4:
                    k1 ^= ((long) data[offset + tailStart + 3] & 0xff) << 24;
                case 3:
                    k1 ^= ((long) data[offset + tailStart + 2] & 0xff) << 16;
                case 2:
                    k1 ^= ((long) data[offset + tailStart + 1] & 0xff) << 8;
                case 1:
                    k1 ^= ((long) data[offset + tailStart] & 0xff);
                    k1 *= C1;
                    k1 = Long.rotateLeft(k1, R1);
                    k1 *= C2;
                    hash ^= k1;
            }

            // finalization
            hash ^= length;
            hash = fmix64(hash);

            return hash;
        }

        private static long fmix64(long h)
        {
            h ^= (h >>> 33);
            h *= 0xff51afd7ed558ccdL;
            h ^= (h >>> 33);
            h *= 0xc4ceb9fe1a85ec53L;
            h ^= (h >>> 33);
            return h;
        }
    }
}
