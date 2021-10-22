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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.array.IntBigArray;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.openjdk.jol.info.ClassLayout;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static it.unimi.dsi.fastutil.HashCommon.murmurHash3;

public class LongDictionaryBuilder
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(LongDictionaryBuilder.class).instanceSize() + ClassLayout.parseClass(LongArrayList.class).instanceSize();
    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;

    private final IntBigArray elementPositionByHash;
    private final LongArrayList elements;

    private int maxFill;
    private int hashMask;

    public LongDictionaryBuilder(int expectedSize)
    {
        checkArgument(expectedSize >= 0, "expectedSize must not be negative");

        int hashSize = arraySize(expectedSize, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashSize);
        this.hashMask = hashSize - 1;

        elements = new LongArrayList(expectedSize);
        elementPositionByHash = new IntBigArray();
        elementPositionByHash.ensureCapacity(hashSize);
        elementPositionByHash.fill(EMPTY_SLOT);
    }

    public void clear()
    {
        elementPositionByHash.fill(EMPTY_SLOT);
        elements.clear();
    }

    public int putIfAbsent(long value)
    {
        int slicePosition;
        long hashPosition = getHashPositionOfElement(value);
        if (elementPositionByHash.get(hashPosition) != EMPTY_SLOT) {
            slicePosition = elementPositionByHash.get(hashPosition);
        }
        else {
            slicePosition = addNewElement(hashPosition, value);
        }
        return slicePosition;
    }

    private long getHashPositionOfElement(long value)
    {
        long hashPosition = getHash(value);
        hashPosition = getMaskedHash(hashPosition);
        while (true) {
            int slicePosition = elementPositionByHash.get(hashPosition);
            if (slicePosition == EMPTY_SLOT) {
                // Doesn't have this element
                return hashPosition;
            }
            if (elements.getLong(slicePosition) == value) {
                return hashPosition;
            }

            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private long getRehashPositionOfElement(long value)
    {
        long hashPosition = getHash(value);
        hashPosition = getMaskedHash(hashPosition);
        while (elementPositionByHash.get(hashPosition) != EMPTY_SLOT) {
            // in Re-hash there is no collision and continue to search until an empty spot is found.
            hashPosition = getMaskedHash(hashPosition + 1);
        }
        return hashPosition;
    }

    private int addNewElement(long hashPosition, long value)
    {
        int newElementPositionInBlock = elements.size();
        elements.add(value);
        elementPositionByHash.set(hashPosition, newElementPositionInBlock);

        // increase capacity, if necessary
        if (elements.size() >= maxFill) {
            rehash(maxFill * 2);
        }

        return newElementPositionInBlock;
    }

    private void rehash(int size)
    {
        int newHashSize = arraySize(size + 1, FILL_RATIO);
        hashMask = newHashSize - 1;
        maxFill = calculateMaxFill(newHashSize);
        elementPositionByHash.ensureCapacity(newHashSize);
        elementPositionByHash.fill(EMPTY_SLOT);

        for (int i = 0; i < elements.size(); i++) {
            elementPositionByHash.set(getRehashPositionOfElement(elements.getLong(i)), i);
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        return maxFill;
    }

    protected long getHash(long value)
    {
        return murmurHash3(value);
    }

    private long getMaskedHash(long rawHash)
    {
        return rawHash & hashMask;
    }

    public long[] elements()
    {
        return elements.elements();
    }

    public long getValue(int index)
    {
        return elements.getLong(index);
    }

    public long getRetainedBytes()
    {
        return INSTANCE_SIZE + elementPositionByHash.sizeOf() + sizeOf(elements.elements());
    }

    public int size()
    {
        return elements.size();
    }
}
