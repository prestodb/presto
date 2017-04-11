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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import org.openjdk.jol.info.ClassLayout;

import java.util.Arrays;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INSUFFICIENT_RESOURCES;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.positionEqualsPosition;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.sizeOf;
import static it.unimi.dsi.fastutil.HashCommon.arraySize;
import static java.util.Objects.requireNonNull;

public class KeyValuePairs
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KeyValuePairs.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;
    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;

    private final BlockBuilder keyBlockBuilder;
    private final Type keyType;

    private final BlockBuilder valueBlockBuilder;
    private final Type valueType;

    private int[] keyPositionByHash;
    private int hashCapacity;
    private int maxFill;
    private int hashMask;

    public KeyValuePairs(Type keyType, Type valueType)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        keyBlockBuilder = this.keyType.createBlockBuilder(new BlockBuilderStatus(), EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        valueBlockBuilder = this.valueType.createBlockBuilder(new BlockBuilderStatus(), EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
        hashCapacity = arraySize(EXPECTED_ENTRIES, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashCapacity);
        this.hashMask = hashCapacity - 1;
        keyPositionByHash = new int[hashCapacity];
        Arrays.fill(keyPositionByHash, EMPTY_SLOT);
    }

    public KeyValuePairs(Block serialized, Type keyType, Type valueType)
    {
        this(keyType, valueType);
        deserialize(requireNonNull(serialized, "serialized is null"));
    }

    public Block getKeys()
    {
        return keyBlockBuilder.build();
    }

    public Block getValues()
    {
        return valueBlockBuilder.build();
    }

    private void deserialize(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(block, block, i, i + 1);
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder mapBlockBuilder = out.beginBlockEntry();
        for (int i = 0; i < keyBlockBuilder.getPositionCount(); i++) {
            keyType.appendTo(keyBlockBuilder, i, mapBlockBuilder);
            valueType.appendTo(valueBlockBuilder, i, mapBlockBuilder);
        }
        out.closeEntry();
    }

    public long estimatedInMemorySize()
    {
        long size = INSTANCE_SIZE;
        size += keyBlockBuilder.getRetainedSizeInBytes();
        size += valueBlockBuilder.getRetainedSizeInBytes();
        size += sizeOf(keyPositionByHash);
        return size;
    }

    /**
     * Only add this key value pair if we haven't seen this key before.
     * Otherwise, ignore it.
     */
    public void add(Block key, Block value, int keyPosition, int valuePosition)
    {
        if (!keyExists(key, keyPosition)) {
            addKey(key, keyPosition);
            if (value.isNull(valuePosition)) {
                valueBlockBuilder.appendNull();
            }
            else {
                valueType.appendTo(value, valuePosition, valueBlockBuilder);
            }
        }
    }

    private boolean keyExists(Block key, int position)
    {
        checkArgument(position >= 0, "position is negative");
        return keyPositionByHash[getHashPositionOfKey(key, position)] != EMPTY_SLOT;
    }

    private void addKey(Block key, int position)
    {
        checkArgument(position >= 0, "position is negative");
        keyType.appendTo(key, position, keyBlockBuilder);
        int hashPosition = getHashPositionOfKey(key, position);
        if (keyPositionByHash[hashPosition] == EMPTY_SLOT) {
            keyPositionByHash[hashPosition] = keyBlockBuilder.getPositionCount() - 1;
            if (keyBlockBuilder.getPositionCount() >= maxFill) {
                rehash();
            }
        }
    }

    private int getHashPositionOfKey(Block key, int position)
    {
        int hashPosition = getMaskedHash(hashPosition(keyType, key, position));
        while (true) {
            if (keyPositionByHash[hashPosition] == EMPTY_SLOT) {
                return hashPosition;
            }
            else if (positionEqualsPosition(keyType, keyBlockBuilder, keyPositionByHash[hashPosition], key, position)) {
                return hashPosition;
            }
            hashPosition = getMaskedHash(hashPosition + 1);
        }
    }

    private void rehash()
    {
        long newCapacityLong = hashCapacity * 2L;
        if (newCapacityLong > Integer.MAX_VALUE) {
            throw new PrestoException(GENERIC_INSUFFICIENT_RESOURCES, "Size of hash table cannot exceed 1 billion entries");
        }
        int newCapacity = (int) newCapacityLong;
        hashCapacity = newCapacity;
        hashMask = newCapacity - 1;
        maxFill = calculateMaxFill(newCapacity);
        keyPositionByHash = new int[newCapacity];
        Arrays.fill(keyPositionByHash, EMPTY_SLOT);
        for (int position = 0; position < keyBlockBuilder.getPositionCount(); position++) {
            keyPositionByHash[getHashPositionOfKey(keyBlockBuilder, position)] = position;
        }
    }

    private static int calculateMaxFill(int hashSize)
    {
        checkArgument(hashSize > 0, "hashSize must be greater than 0");
        int maxFill = (int) Math.ceil(hashSize * FILL_RATIO);
        if (maxFill == hashSize) {
            maxFill--;
        }
        checkArgument(hashSize > maxFill, "hashSize must be larger than maxFill");
        return maxFill;
    }

    private int getMaskedHash(long rawHash)
    {
        return (int) (rawHash & hashMask);
    }
}
