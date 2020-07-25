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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
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

public final class SetOfValues
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SetOfValues.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;
    private static final float FILL_RATIO = 0.75f;
    private static final int EMPTY_SLOT = -1;

    private final BlockBuilder valueBlockBuilder;
    private final Type valueType;

    private int[] valuePositionByHash;
    private int hashCapacity;
    private int maxFill;
    private int hashMask;

    public SetOfValues(Type valueType)
    {
        this.valueType = requireNonNull(valueType, "valueType is null");
        valueBlockBuilder = this.valueType.createBlockBuilder(null, EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
        hashCapacity = arraySize(EXPECTED_ENTRIES, FILL_RATIO);
        this.maxFill = calculateMaxFill(hashCapacity);
        this.hashMask = hashCapacity - 1;
        valuePositionByHash = new int[hashCapacity];
        Arrays.fill(valuePositionByHash, EMPTY_SLOT);
    }

    public SetOfValues(Block serialized, Type elementType)
    {
        this(elementType);
        deserialize(requireNonNull(serialized, "serialized is null"));
    }

    public Block getvalues()
    {
        return valueBlockBuilder.build();
    }

    private void deserialize(Block block)
    {
        for (int i = 0; i < block.getPositionCount(); i++) {
            add(block, i);
        }
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder arrayBlockBuilder = out.beginBlockEntry();
        for (int i = 0; i < valueBlockBuilder.getPositionCount(); i++) {
            valueType.appendTo(valueBlockBuilder, i, arrayBlockBuilder);
        }
        out.closeEntry();
    }

    public long estimatedInMemorySize()
    {
        long size = INSTANCE_SIZE;
        size += valueBlockBuilder.getRetainedSizeInBytes();
        size += sizeOf(valuePositionByHash);
        return size;
    }

    public void add(Block value, int valuePosition)
    {
        int hashPosition = getHashPositionOfvalue(value, valuePosition);
        if (valuePositionByHash[hashPosition] == EMPTY_SLOT) {
            valueType.appendTo(value, valuePosition, valueBlockBuilder);
            valuePositionByHash[hashPosition] = valueBlockBuilder.getPositionCount() - 1;
            if (valueBlockBuilder.getPositionCount() >= maxFill) {
                rehash();
            }
        }
    }

    private int getHashPositionOfvalue(Block value, int position)
    {
        int hashPosition = getMaskedHash(hashPosition(valueType, value, position));
        while (true) {
            if (valuePositionByHash[hashPosition] == EMPTY_SLOT) {
                return hashPosition;
            }
            else if (positionEqualsPosition(valueType, valueBlockBuilder, valuePositionByHash[hashPosition], value, position)) {
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
        valuePositionByHash = new int[newCapacity];
        Arrays.fill(valuePositionByHash, EMPTY_SLOT);
        for (int position = 0; position < valueBlockBuilder.getPositionCount(); position++) {
            valuePositionByHash[getHashPositionOfvalue(valueBlockBuilder, position)] = position;
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
