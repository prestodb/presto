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

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceInput;
import org.openjdk.jol.info.ClassLayout;

import java.util.BitSet;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A level of abstraction over the bitmaps used in sketches such as SFM.
 * Abstractly, these are essentially fixed-length arrays of booleans that support flipping and applying randomized response.
 * This is implemented as a wrapper around Java's BitSet.
 * <p>
 * Note: The byte arrays in toBytes() and fromSliceInput() are variable-length.
 * Trailing zeros are implicitly truncated in these functions.
 * The fixed-length nature of the bitmap comes into play in flipAll (randomized response),
 * where every bit from 0 to length-1 must be flipped with a fixed probability.
 */
public class Bitmap
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(Bitmap.class).instanceSize();
    private static final int BITSET_INSTANCE_SIZE = ClassLayout.parseClass(BitSet.class).instanceSize();

    private final BitSet bitSet;
    private final int length;

    public Bitmap(int length)
    {
        checkArgument(length > 0, "length must be positive");
        bitSet = new BitSet(length);
        this.length = length;
    }

    private Bitmap(int length, BitSet bitSet)
    {
        requireNonNull(bitSet, "bitSet cannot be null");
        checkArgument(length >= bitSet.length(), "bitmap size must be large enough to cover existing bits");
        this.bitSet = bitSet;
        this.length = length;
    }

    public static Bitmap fromBytes(int length, byte[] bytes)
    {
        return new Bitmap(length, BitSet.valueOf(bytes));
    }

    public static Bitmap fromSliceInput(SliceInput input, int byteCount, int length)
    {
        checkArgument(byteCount >= 0, "byteCount must be nonnegative");
        if (byteCount == 0) {
            return new Bitmap(length);
        }

        byte[] bytes = new byte[byteCount];
        input.readBytes(bytes);
        return Bitmap.fromBytes(length, bytes);
    }

    public byte[] toBytes()
    {
        return bitSet.toByteArray();
    }

    /**
     * Length of toBytes()
     */
    public int byteLength()
    {
        // per https://docs.oracle.com/javase/8/docs/api/java/util/BitSet.html#toByteArray--
        return (bitSet.length() + 7) / 8;
    }

    @Override
    public Bitmap clone()
    {
        return Bitmap.fromBytes(length, bitSet.toByteArray());
    }

    public long getRetainedSizeInBytes()
    {
        // Under the hood, BitSet stores a long[] array of BitSet.size() bits
        return INSTANCE_SIZE + BITSET_INSTANCE_SIZE + SizeOf.sizeOfLongArray(bitSet.size() / Long.SIZE);
    }

    public boolean getBit(int position)
    {
        return bitSet.get(position);
    }

    /**
     * The number of 1-bits in the bitmap
     */
    public int getBitCount()
    {
        return bitSet.cardinality();
    }

    /**
     * Randomly (and independently) flip all bits with specified probability
     */
    public void flipAll(double probability, RandomizationStrategy randomizationStrategy)
    {
        for (int i = 0; i < length; i++) {
            flipBit(i, probability, randomizationStrategy);
        }
    }

    /**
     * Deterministically flips the bit at a given position
     */
    public void flipBit(int position)
    {
        bitSet.flip(position);
    }

    /**
     * Randomly flips the bit at a given position with specified probability
     */
    public void flipBit(int position, double probability, RandomizationStrategy randomizationStrategy)
    {
        if (randomizationStrategy.nextBoolean(probability)) {
            flipBit(position);
        }
    }

    /**
     * The nominal fixed length of the bitmap (actual stored size may vary)
     */
    public int length()
    {
        return length;
    }

    /**
     * Explicitly set the value of the bit at a given position
     */
    public void setBit(int position, boolean value)
    {
        bitSet.set(position, value);
    }

    public void or(Bitmap other)
    {
        requireNonNull(other, "cannot combine with null Bitmap");
        checkArgument(length() == other.length(), "cannot OR two bitmaps of different size");

        bitSet.or(other.bitSet);
    }

    public void xor(Bitmap other)
    {
        requireNonNull(other, "cannot combine with null Bitmap");
        checkArgument(length() == other.length(), "cannot XOR two bitmaps of different size");

        bitSet.xor(other.bitSet);
    }
}
