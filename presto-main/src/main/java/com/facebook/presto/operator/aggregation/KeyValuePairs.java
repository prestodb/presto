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

import com.facebook.presto.array.ObjectBigArray;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static java.util.Objects.requireNonNull;

public class KeyValuePairs
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KeyValuePairs.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;

    private final TypedSet keySet;
    private final BlockBuilder keyBlockBuilder;
    private final Type keyType;

    private final BlockBuilder valueBlockBuilder;
    private final Type valueType;

    private final boolean isMultiValue;

    public KeyValuePairs(Type keyType, Type valueType)
    {
        this(keyType, valueType, false);
    }

    public KeyValuePairs(Type keyType, Type valueType, boolean isMultiValue)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.keySet = new TypedSet(keyType, EXPECTED_ENTRIES);
        keyBlockBuilder = this.keyType.createBlockBuilder(new BlockBuilderStatus(), EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        valueBlockBuilder = this.valueType.createBlockBuilder(new BlockBuilderStatus(), EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));

        this.isMultiValue = isMultiValue;
    }

    public KeyValuePairs(Block serialized, Type keyType, Type valueType, boolean isMultiValue)
    {
        this(keyType, valueType, isMultiValue);
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

    public Block serialize()
    {
        Block keys = keyBlockBuilder.build();
        Block values = valueBlockBuilder.build();
        BlockBuilder blockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueType), new BlockBuilderStatus(), keys.getPositionCount() * 2);
        for (int i = 0; i < keys.getPositionCount(); i++) {
            keyType.appendTo(keys, i, blockBuilder);
            valueType.appendTo(values, i, blockBuilder);
        }
        return blockBuilder.build();
    }

    /**
     * Serialize as a map: map(key, value)
     */
    public Block toMapNativeEncoding()
    {
        if (isMultiValue) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "This KeyValuePairs is multimap.");
        }

        return serialize();
    }

    /**
     * Serialize as a multimap: map(key, array(value)), each key can be associated with multiple values
     */
    public Block toMultimapNativeEncoding()
    {
        if (!isMultiValue) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "This KeyValuePairs is not multimap.");
        }

        Block keys = keyBlockBuilder.build();
        Block values = valueBlockBuilder.build();

        // Merge values of the same key into an array
        BlockBuilder distinctKeyBlockBuilder = keyType.createBlockBuilder(new BlockBuilderStatus(), keys.getPositionCount(), expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        ObjectBigArray<BlockBuilder> valueArrayBlockBuilders = new ObjectBigArray<>();
        valueArrayBlockBuilders.ensureCapacity(keys.getPositionCount());
        TypedSet keySet = new TypedSet(keyType, keys.getPositionCount());
        for (int keyValueIndex = 0; keyValueIndex < keys.getPositionCount(); keyValueIndex++) {
            if (!keySet.contains(keys, keyValueIndex)) {
                keySet.add(keys, keyValueIndex);
                keyType.appendTo(keys, keyValueIndex, distinctKeyBlockBuilder);
                BlockBuilder valueArrayBuilder = valueType.createBlockBuilder(new BlockBuilderStatus(), 10, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
                valueArrayBlockBuilders.set(keySet.positionOf(keys, keyValueIndex), valueArrayBuilder);
            }
            valueType.appendTo(values, keyValueIndex, valueArrayBlockBuilders.get(keySet.positionOf(keys, keyValueIndex)));
        }

        // Write keys and value arrays into one Block
        Block distinctKeys = distinctKeyBlockBuilder.build();
        Type valueArrayType = new ArrayType(valueType);
        BlockBuilder multimapBlockBuilder = new InterleavedBlockBuilder(ImmutableList.of(keyType, valueArrayType), new BlockBuilderStatus(), distinctKeyBlockBuilder.getPositionCount());
        for (int i = 0; i < distinctKeys.getPositionCount(); i++) {
            keyType.appendTo(distinctKeys, i, multimapBlockBuilder);
            valueArrayType.writeObject(multimapBlockBuilder, valueArrayBlockBuilders.get(i).build());
        }

        return multimapBlockBuilder.build();
    }

    public long estimatedInMemorySize()
    {
        long size = INSTANCE_SIZE;
        size += keyBlockBuilder.getRetainedSizeInBytes();
        size += valueBlockBuilder.getRetainedSizeInBytes();
        size += keySet.getRetainedSizeInBytes();
        return size;
    }

    /**
     * Only add this key value pair if we are in multi-value mode or we haven't met this key before.
     * Otherwise, ignore it.
     */
    public void add(Block key, Block value, int keyPosition, int valuePosition)
    {
        if (isMultiValue || !keySet.contains(key, keyPosition)) {
            keySet.add(key, keyPosition);
            keyType.appendTo(key, keyPosition, keyBlockBuilder);
            if (value.isNull(valuePosition)) {
                valueBlockBuilder.appendNull();
            }
            else {
                valueType.appendTo(value, valuePosition, valueBlockBuilder);
            }
        }
    }
}
