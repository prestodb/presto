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

import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;

import static com.facebook.presto.operator.GroupByHash.createGroupByHash;
import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static com.google.common.base.Preconditions.checkNotNull;

public class KeyValuePairs
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KeyValuePairs.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;

    private final GroupByHash keysHash;
    private final BlockBuilder keyBlockBuilder;
    private final Type keyType;

    private final BlockBuilder valueBlockBuilder;
    private final Type valueType;

    public KeyValuePairs(Type keyType, Type valueType)
    {
        checkNotNull(keyType, "keyType is null");
        checkNotNull(valueType, "valueType is null");

        this.keyType = keyType;
        this.valueType = valueType;
        keysHash = createGroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.empty(), EXPECTED_ENTRIES);
        BlockBuilderStatus keyBlockBuilderStatus = new BlockBuilderStatus();
        keyBlockBuilder = this.keyType.createBlockBuilder(keyBlockBuilderStatus, EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        valueBlockBuilder = this.valueType.createBlockBuilder(new BlockBuilderStatus(), EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
    }

    public KeyValuePairs(Slice serialized, Type keyType, Type valueType)
    {
        checkNotNull(serialized, "serialized is null");
        checkNotNull(keyType, "keyType is null");
        checkNotNull(valueType, "valueType is null");

        this.keyType = keyType;
        this.valueType = valueType;
        keysHash = createGroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.empty(), EXPECTED_ENTRIES);
        BlockBuilderStatus keyBlockBuilderStatus = new BlockBuilderStatus();
        keyBlockBuilder = this.keyType.createBlockBuilder(keyBlockBuilderStatus, EXPECTED_ENTRIES, expectedValueSize(keyType, EXPECTED_ENTRY_SIZE));
        valueBlockBuilder = this.valueType.createBlockBuilder(new BlockBuilderStatus(), EXPECTED_ENTRIES, expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
        deserialize(serialized);
    }

    public Block getKeys()
    {
        return keyBlockBuilder.build();
    }

    public Block getValues()
    {
        return valueBlockBuilder.build();
    }

    private void deserialize(Slice serialized)
    {
        Block block = TypeUtils.readStructuralBlock(serialized);
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(block, block, i, i + 1);
        }
    }

    public Slice serialize()
    {
        Block values = valueBlockBuilder.build();
        Block keys = keyBlockBuilder.build();
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), keys.getSizeInBytes() + values.getSizeInBytes());
        for (int i = 0; i < keys.getPositionCount(); i++) {
            keyType.appendTo(keys, i, blockBuilder);
            valueType.appendTo(values, i, blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }

    public long estimatedInMemorySize()
    {
        long size = INSTANCE_SIZE;
        size += Math.max(EXPECTED_ENTRIES * expectedValueSize(keyType, EXPECTED_ENTRY_SIZE), keyBlockBuilder.getSizeInBytes());
        size += Math.max(EXPECTED_ENTRIES * expectedValueSize(valueType, EXPECTED_ENTRY_SIZE), valueBlockBuilder.getSizeInBytes());
        // TODO: We need an optimized version of GroupByHash that doesn't allocate a full PageBuilder
        size += Math.max(DEFAULT_MAX_BLOCK_SIZE_IN_BYTES, keysHash.getEstimatedSize());
        return size;
    }

    public void add(Block key, Block value, int keyPosition, int valuePosition)
    {
        Page page = new Page(key);
        if (!keysHash.contains(keyPosition, page)) {
            keysHash.putIfAbsent(keyPosition, page);
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
