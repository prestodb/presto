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

package io.prestosql.spi.block;

import io.airlift.slice.Slice;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.lang.invoke.MethodHandle;
import java.util.function.BiConsumer;

import static io.airlift.slice.SizeOf.sizeOf;
import static io.airlift.slice.SizeOf.sizeOfIntArray;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.block.AbstractMapBlock.HASH_MULTIPLIER;
import static io.prestosql.spi.block.MapBlockBuilder.computePosition;
import static java.lang.String.format;

public class SingleMapBlock
        extends AbstractSingleMapBlock
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SingleMapBlock.class).instanceSize();

    private final int offset;
    private final int positionCount;
    private final Block keyBlock;
    private final Block valueBlock;
    private final int[] hashTable;
    final Type keyType;
    private final MethodHandle keyNativeHashCode;
    private final MethodHandle keyBlockNativeEquals;

    SingleMapBlock(int offset, int positionCount, Block keyBlock, Block valueBlock, int[] hashTable, Type keyType, MethodHandle keyNativeHashCode, MethodHandle keyBlockNativeEquals)
    {
        this.offset = offset;
        this.positionCount = positionCount;
        this.keyBlock = keyBlock;
        this.valueBlock = valueBlock;
        this.hashTable = hashTable;
        this.keyType = keyType;
        this.keyNativeHashCode = keyNativeHashCode;
        this.keyBlockNativeEquals = keyBlockNativeEquals;
    }

    @Override
    public int getPositionCount()
    {
        return positionCount;
    }

    @Override
    public long getSizeInBytes()
    {
        return keyBlock.getRegionSizeInBytes(offset / 2, positionCount / 2) +
                valueBlock.getRegionSizeInBytes(offset / 2, positionCount / 2) +
                sizeOfIntArray(positionCount / 2 * HASH_MULTIPLIER);
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + keyBlock.getRetainedSizeInBytes() + valueBlock.getRetainedSizeInBytes() + sizeOf(hashTable);
    }

    @Override
    public void retainedBytesForEachPart(BiConsumer<Object, Long> consumer)
    {
        consumer.accept(keyBlock, keyBlock.getRetainedSizeInBytes());
        consumer.accept(valueBlock, valueBlock.getRetainedSizeInBytes());
        consumer.accept(hashTable, sizeOf(hashTable));
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
        return keyBlock;
    }

    @Override
    Block getRawValueBlock()
    {
        return valueBlock;
    }

    @Override
    public String toString()
    {
        return format("SingleMapBlock{positionCount=%d}", getPositionCount());
    }

    @Override
    public Block getLoadedBlock()
    {
        if (keyBlock != keyBlock.getLoadedBlock()) {
            // keyBlock has to be loaded since MapBlock constructs hash table eagerly.
            throw new IllegalStateException();
        }

        Block loadedValueBlock = valueBlock.getLoadedBlock();
        if (loadedValueBlock == valueBlock) {
            return this;
        }
        return new SingleMapBlock(
                offset,
                positionCount,
                keyBlock,
                loadedValueBlock,
                hashTable,
                keyType,
                keyNativeHashCode,
                keyBlockNativeEquals);
    }

    int[] getHashTable()
    {
        return hashTable;
    }

    /**
     * @return position of the value under {@code nativeValue} key. -1 when key is not found.
     */
    public int seekKey(Object nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

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
                match = (Boolean) keyBlockNativeEquals.invoke(keyBlock, offset / 2 + keyPosition, nativeValue);
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

    public int seekKeyExact(long nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

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
                match = (Boolean) keyBlockNativeEquals.invokeExact(keyBlock, offset / 2 + keyPosition, nativeValue);
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

    public int seekKeyExact(boolean nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

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
                match = (Boolean) keyBlockNativeEquals.invokeExact(keyBlock, offset / 2 + keyPosition, nativeValue);
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

    public int seekKeyExact(double nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

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
                match = (Boolean) keyBlockNativeEquals.invokeExact(keyBlock, offset / 2 + keyPosition, nativeValue);
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

    public int seekKeyExact(Slice nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

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
                match = (Boolean) keyBlockNativeEquals.invokeExact(keyBlock, offset / 2 + keyPosition, nativeValue);
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

    public int seekKeyExact(Block nativeValue)
    {
        if (positionCount == 0) {
            return -1;
        }

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
                match = (Boolean) keyBlockNativeEquals.invokeExact(keyBlock, offset / 2 + keyPosition, nativeValue);
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
        if (throwable instanceof PrestoException) {
            throw (PrestoException) throwable;
        }
        throw new PrestoException(GENERIC_INTERNAL_ERROR, throwable);
    }

    private static void checkNotIndeterminate(Boolean equalsResult)
    {
        if (equalsResult == null) {
            throw new PrestoException(NOT_SUPPORTED, "map key cannot be null or contain nulls");
        }
    }
}
