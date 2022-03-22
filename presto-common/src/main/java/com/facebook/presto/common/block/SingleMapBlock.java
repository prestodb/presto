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

package com.facebook.presto.common.block;

import com.facebook.presto.common.GenericInternalException;
import com.facebook.presto.common.NotSupportedException;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.function.BiConsumer;

import static com.facebook.presto.common.block.AbstractMapBlock.HASH_MULTIPLIER;
import static com.facebook.presto.common.block.MapBlockBuilder.computePosition;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static java.lang.String.format;

public class SingleMapBlock
        extends AbstractSingleMapBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleMapBlock.class).instanceSize();

    private final int positionInMap;
    private final int offset;
    private final int positionCount;  // The number of keys in this single map * 2
    private final AbstractMapBlock mapBlock;

    SingleMapBlock(int positionInMap, int offset, int positionCount, AbstractMapBlock mapBlock)
    {
        this.positionInMap = positionInMap;
        this.offset = offset;
        this.positionCount = positionCount;
        this.mapBlock = mapBlock;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return mapBlock.getRawKeyBlock().getRegionSizeInBytes(offset / 2, positionCount / 2) +
                mapBlock.getRawValueBlock().getRegionSizeInBytes(offset / 2, positionCount / 2) +
                sizeOfIntArray(positionCount / 2 * HASH_MULTIPLIER);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE +
                mapBlock.getRawKeyBlock().getRetainedSizeInBytes() +
                mapBlock.getRawValueBlock().getRetainedSizeInBytes() +
                mapBlock.getHashTables().getRetainedSizeInBytes();
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(mapBlock.getRawKeyBlock(), mapBlock.getRawKeyBlock().getRetainedSizeInBytes());
        consumer.accept(mapBlock.getRawValueBlock(), mapBlock.getRawValueBlock().getRetainedSizeInBytes());
        consumer.accept(mapBlock.getHashTables(), mapBlock.getHashTables().getRetainedSizeInBytes());
        consumer.accept(this, (long) INSTANCE_SIZE);
    }

    @Override
    public String getEncodingName()
    {
        return SingleMapBlockEncoding.NAME;
    }

    @Override
    public int getOffset()
    {
        return offset;
    }

    @Override
    Block getRawKeyBlock()
    {
        return mapBlock.getRawKeyBlock();
    }

    @Override
    Block getRawValueBlock()
    {
        return mapBlock.getRawValueBlock();
    }

    @Override
    public String toString()
    {
        return format("SingleMapBlock(%d){positionCount=%d}", hashCode(), getPositionCount());
    }

    @Override
    public Block getLoadedBlock()
    {
        if (mapBlock.getRawKeyBlock() != mapBlock.getRawKeyBlock().getLoadedBlock()) {
            // keyBlock has to be loaded since MapBlock constructs hash table eagerly.
            throw new IllegalStateException();
        }

        Block loadedValueBlock = mapBlock.getRawValueBlock().getLoadedBlock();
        if (loadedValueBlock == mapBlock.getRawValueBlock()) {
            return this;
        }
        return new SingleMapBlock(
                positionInMap,
                offset,
                positionCount,
                mapBlock);
    }

    public Block getKeyBlock()
    {
        return mapBlock.getRawKeyBlock().getRegion(mapBlock.getOffset(positionInMap), positionCount / 2);
    }

    public Block getValueBlock()
    {
        return mapBlock.getRawValueBlock().getRegion(positionInMap, positionCount / 2);
    }

    public Block getBaseMapBlock()
    {
        return mapBlock;
    }

    public int getPositionInMap()
    {
        return positionInMap;
    }

    @Override
    public Block appendNull()
    {
        throw new UnsupportedOperationException("SingleMapBlock does not support appendNull()");
    }

    @Nullable
    int[] getHashTable()
    {
        return mapBlock.getHashTables().get();
    }

    /**
     * @return position of the value under {@code nativeValue} key. -1 when key is not found.
     */
    public int seekKey(Object nativeValue, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded(keyBlockHashCode);
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) keyNativeHashCode.invoke(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyBlockNativeEquals.invoke(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    // The next 5 seekKeyExact functions are the same as seekKey
    // except MethodHandle.invoke is replaced with invokeExact.

    public int seekKeyExact(long nativeValue, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded(keyBlockHashCode);
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) keyNativeHashCode.invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyBlockNativeEquals.invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(boolean nativeValue, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded(keyBlockHashCode);
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) keyNativeHashCode.invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyBlockNativeEquals.invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(double nativeValue, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded(keyBlockHashCode);
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) keyNativeHashCode.invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyBlockNativeEquals.invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(Slice nativeValue, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded(keyBlockHashCode);
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) keyNativeHashCode.invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                Block rawKeyBlock = mapBlock.getRawKeyBlock();

                if (rawKeyBlock.getSliceLength(offset / 2 + keyPosition) != nativeValue.length()) {
                    match = false;
                }
                else {
                    match = rawKeyBlock.bytesEqual(offset / 2 + keyPosition, 0, nativeValue, 0, nativeValue.length());
                }
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    public int seekKeyExact(Block nativeValue, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals, MethodHandle keyBlockHashCode)
    {
        if (positionCount == 0) {
            return -1;
        }

        mapBlock.ensureHashTableLoaded(keyBlockHashCode);
        int[] hashTable = mapBlock.getHashTables().get();

        long hashCode;
        try {
            hashCode = (long) keyNativeHashCode.invokeExact(nativeValue);
        }
        catch (Throwable throwable) {
            throw handleThrowable(throwable);
        }

        int hashTableOffset = offset / 2 * HASH_MULTIPLIER;
        int hashTableSize = positionCount / 2 * HASH_MULTIPLIER;
        int position = computePosition(hashCode, hashTableSize);
        while (true) {
            int keyPosition = hashTable[hashTableOffset + position];
            if (keyPosition == -1) {
                return -1;
            }
            Boolean match;
            try {
                // assuming maps with indeterminate keys are not supported
                match = (Boolean) keyBlockNativeEquals.invokeExact(mapBlock.getRawKeyBlock(), offset / 2 + keyPosition, nativeValue);
            }
            catch (Throwable throwable) {
                throw handleThrowable(throwable);
            }
            checkNotIndeterminate(match);
            if (match) {
                return keyPosition * 2 + 1;
            }
            position++;
            if (position == hashTableSize) {
                position = 0;
            }
        }
    }

    private static RuntimeException handleThrowable(Throwable throwable)
    {
        if (throwable instanceof Error) {
            throw (Error) throwable;
        }
        throw new GenericInternalException(throwable);
    }

    private static void checkNotIndeterminate(Boolean equalsResult)
    {
        if (equalsResult == null) {
            throw new NotSupportedException("map key cannot be null or contain nulls");
        }
    }
}
