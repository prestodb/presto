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
package com.facebook.presto.bloomfilter;

import com.google.common.hash.Hashing;

import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

public class BloomFilterImpl
        extends BloomFilter
        implements Serializable
{
    private int numHashFunctions;

    private boolean containsNull;

    private BitArray bits;

    public BloomFilterImpl(int numHashFunctions, long numBits)
    {
        this(new BitArray(numBits), numHashFunctions);
    }

    public BloomFilterImpl(BitArray bits, int numHashFunctions)
    {
        this.bits = bits;
        this.numHashFunctions = numHashFunctions;
    }

    public BloomFilterImpl() {}

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }

        if (other == null || !(other instanceof BloomFilterImpl)) {
            return false;
        }

        BloomFilterImpl that = (BloomFilterImpl) other;

        return this.numHashFunctions == that.numHashFunctions && this.bits.equals(that.bits);
    }

    @Override
    public int hashCode()
    {
        return bits.hashCode() * 31 + numHashFunctions;
    }

    @Override
    public double expectedFpp()
    {
        return Math.pow((double) bits.cardinality() / bits.bitSize(), numHashFunctions);
    }

    @Override
    public long bitSize()
    {
        return bits.bitSize();
    }

    @Override
    public long cardinality()
    {
        return bits.cardinality();
    }

    @Override
    public boolean clear()
    {
        bits.clear();
        return true;
    }

    @Override
    public boolean put(Object item)
    {
        if (item instanceof String) {
            return putString((String) item);
        }
        else if (item instanceof byte[]) {
            return putBinary((byte[]) item);
        }
        else {
            return putLong(Utils.integralToLong(item));
        }
    }

    @Override
    public boolean putString(String item)
    {
        return putBinary(Utils.getBytesFromUTF8String(item));
    }

    @Override
    public boolean putBinary(byte[] item)
    {
        int h1 = Hashing.murmur3_32().hashBytes(item, 0, item.length).asInt();
        int h2 = Hashing.murmur3_32(h1).hashBytes(item, 0, item.length).asInt();

        long bitSize = bits.bitSize();
        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bits.set(combinedHash % bitSize);
        }
        return bitsChanged;
    }

    @Override
    public boolean mightContainString(String item)
    {
        return mightContainBinary(Utils.getBytesFromUTF8String(item));
    }

    @Override
    public boolean mightContainBinary(byte[] item)
    {
        int h1 = Hashing.murmur3_32().hashBytes(item, 0, item.length).asInt();
        int h2 = Hashing.murmur3_32(h1).hashBytes(item, 0, item.length).asInt();

        long bitSize = bits.bitSize();
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            if (!bits.get(combinedHash % bitSize)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean putNull()
    {
        boolean changed = !containsNull;
        containsNull = true;
        return changed;
    }

    @Override
    public boolean putLong(long item)
    {
        // Here we first hash the input long element into 2 int hash values, h1 and h2, then produce n
        // hash values by `h1 + i * h2` with 1 <= i <= numHashFunctions.
        // Note that `CountMinSketch` use a different strategy, it hash the input long element with
        // every i to produce n hash values.
        // TODO: the strategy of `CountMinSketch` looks more advanced, should we follow it here?
        int h1 = Hashing.murmur3_32().hashLong(item).asInt();
        int h2 = Hashing.murmur3_32(h1).hashLong(item).asInt();

        long bitSize = bits.bitSize();
        boolean bitsChanged = false;
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            bitsChanged |= bits.set(combinedHash % bitSize);
        }
        return bitsChanged;
    }

    @Override
    public boolean mightContainLong(long item)
    {
        int h1 = Hashing.murmur3_32().hashLong(item).asInt();
        int h2 = Hashing.murmur3_32(h1).hashLong(item).asInt();

        long bitSize = bits.bitSize();
        for (int i = 1; i <= numHashFunctions; i++) {
            int combinedHash = h1 + (i * h2);
            // Flip all the bits if it's negative (guaranteed positive number)
            if (combinedHash < 0) {
                combinedHash = ~combinedHash;
            }
            if (!bits.get(combinedHash % bitSize)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean mightContain(Object item)
    {
        if (item instanceof String) {
            return mightContainString((String) item);
        }
        else if (item instanceof byte[]) {
            return mightContainBinary((byte[]) item);
        }
        else {
            return mightContainLong(Utils.integralToLong(item));
        }
    }

    @Override
    public boolean containNull()
    {
        return containsNull;
    }

    @Override
    public boolean isCompatible(BloomFilter other)
    {
        if (other == null) {
            return false;
        }

        if (!(other instanceof BloomFilterImpl)) {
            return false;
        }

        BloomFilterImpl that = (BloomFilterImpl) other;
        return this.bitSize() == that.bitSize() && this.numHashFunctions == that.numHashFunctions;
    }

    @Override
    public BloomFilter mergeInPlace(BloomFilter other) throws IncompatibleMergeException
    {
        // Duplicates the logic of `isCompatible` here to provide better error message.
        if (other == null) {
            throw new IncompatibleMergeException("Cannot merge null bloom filter");
        }
        if (!(other instanceof BloomFilterImpl)) {
            throw new IncompatibleMergeException(
                    "Cannot merge bloom filter of class " + other.getClass().getName());
        }

        BloomFilterImpl that = (BloomFilterImpl) other;
        if (this.bitSize() != that.bitSize()) {
            throw new IncompatibleMergeException("Cannot merge bloom filters with different bit size");
        }

        if (this.numHashFunctions != that.numHashFunctions) {
            throw new IncompatibleMergeException(
                    "Cannot merge bloom filters with different number of hash functions");
        }

        this.containsNull = this.containsNull || that.containsNull;
        this.bits.putAll(that.bits);
        return this;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException
    {
        DataOutputStream dos = new DataOutputStream(out);

        dos.writeInt(Version.V1.getVersionNumber());
        dos.writeBoolean(containsNull);
        dos.writeInt(numHashFunctions);
        bits.writeTo(dos);
    }

    private void readFrom0(InputStream in) throws IOException
    {
        DataInputStream dis = new DataInputStream(in);

        int version = dis.readInt();
        if (version != Version.V1.getVersionNumber()) {
            throw new IOException("Unexpected Bloom filter version number (" + version + ")");
        }

        this.containsNull = dis.readBoolean();
        this.numHashFunctions = dis.readInt();
        this.bits = BitArray.readFrom(dis);
    }

    public static BloomFilterImpl readFrom(InputStream in) throws IOException
    {
        BloomFilterImpl filter = new BloomFilterImpl();
        filter.readFrom0(in);
        return filter;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        writeTo(out);
    }

    private void readObject(ObjectInputStream in) throws IOException
    {
        readFrom0(in);
    }

    @Override
    public byte[] toByteArray() throws IOException
    {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writeTo(out);
            return out.toByteArray();
        }
    }
}
