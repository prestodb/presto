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

import org.apache.hive.common.util.Murmur3;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

public class BloomFilter
{
    protected BloomFilter.BitSet bitSet;
    protected int numBits;
    protected int numHashFunctions;
    public static final int START_OF_SERIALIZED_LONGS = 5;

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
        long hash64 = val == null ? 2862933555777941757L : Murmur3.hash64(val, offset, length);
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
        long hash64 = val == null ? 2862933555777941757L : Murmur3.hash64(val, offset, length);
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

    public static void serialize(OutputStream out, BloomFilter bloomFilter) throws IOException
    {
        DataOutputStream dataOutputStream = new DataOutputStream(out);
        dataOutputStream.writeByte(bloomFilter.numHashFunctions);
        dataOutputStream.writeInt(bloomFilter.getBitSet().length);
        long[] var3 = bloomFilter.getBitSet();
        int var4 = var3.length;

        for (int var5 = 0; var5 < var4; ++var5) {
            long value = var3[var5];
            dataOutputStream.writeLong(value);
        }
    }

    public static org.apache.hive.common.util.BloomFilter deserialize(InputStream in) throws IOException
    {
        if (in == null) {
            throw new IOException("Input stream is null");
        }
        else {
            try {
                DataInputStream dataInputStream = new DataInputStream(in);
                int numHashFunc = dataInputStream.readByte();
                int numLongs = dataInputStream.readInt();
                long[] data = new long[numLongs];

                for (int i = 0; i < numLongs; ++i) {
                    data[i] = dataInputStream.readLong();
                }

                return new org.apache.hive.common.util.BloomFilter(data, numHashFunc);
            }
            catch (RuntimeException var6) {
                IOException io = new IOException("Unable to deserialize BloomFilter");
                io.initCause(var6);
                throw io;
            }
        }
    }

    public static void mergeBloomFilterBytes(byte[] bf1Bytes, int bf1Start, int bf1Length, byte[] bf2Bytes, int bf2Start, int bf2Length)
    {
        if (bf1Length != bf2Length) {
            throw new IllegalArgumentException("bf1Length " + bf1Length + " does not match bf2Length " + bf2Length);
        }
        else {
            int idx;
            for (idx = 0; idx < START_OF_SERIALIZED_LONGS; ++idx) {
                if (bf1Bytes[bf1Start + idx] != bf2Bytes[bf2Start + idx]) {
                    throw new IllegalArgumentException("bf1 NumHashFunctions/NumBits does not match bf2");
                }
            }

            for (idx = START_OF_SERIALIZED_LONGS; idx < bf1Length; ++idx) {
                bf1Bytes[bf1Start + idx] |= bf2Bytes[bf2Start + idx];
            }
        }
    }

    public class BitSet
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
}
