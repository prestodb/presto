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
import com.facebook.presto.spi.PageBuilder;
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
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.google.common.base.Preconditions.checkNotNull;

public class KeyValuePairs
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KeyValuePairs.class).instanceSize();
    public static final int EXPECTED_HASH_SIZE = 10_000;

    private final GroupByHash keysHash;
    private final PageBuilder keyPageBuilder;
    private final Type keyType;

    private final PageBuilder valuePageBuilder;
    private final Type valueType;

    public KeyValuePairs(Type keyType, Type valueType)
    {
        checkNotNull(keyType, "keyType is null");
        checkNotNull(valueType, "valueType is null");

        this.keyType = keyType;
        this.valueType = valueType;
        keysHash = createGroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.empty(), EXPECTED_HASH_SIZE);
        keyPageBuilder = new PageBuilder(ImmutableList.of(this.keyType));
        valuePageBuilder = new PageBuilder(ImmutableList.of(this.valueType));
    }

    public KeyValuePairs(Slice serialized, Type keyType, Type valueType)
    {
        checkNotNull(serialized, "serialized is null");
        checkNotNull(keyType, "keyType is null");
        checkNotNull(valueType, "valueType is null");

        this.keyType = keyType;
        this.valueType = valueType;
        keysHash = createGroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.empty(), 10_000);
        keyPageBuilder = new PageBuilder(ImmutableList.of(this.keyType));
        valuePageBuilder = new PageBuilder(ImmutableList.of(this.valueType));
        deserialize(serialized);
    }

    public Block getKeys()
    {
        return keyPageBuilder.getBlockBuilder(0).build();
    }

    public Block getValues()
    {
        return valuePageBuilder.getBlockBuilder(0).build();
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
        Block values = valuePageBuilder.getBlockBuilder(0).build();
        Block keys = keyPageBuilder.getBlockBuilder(0).build();
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), keys.getSizeInBytes() + values.getSizeInBytes());
        for (int i = 0; i < keys.getPositionCount(); i++) {
            keyType.appendTo(keys, i, blockBuilder);
            valueType.appendTo(values, i, blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + keyPageBuilder.getSizeInBytes() + valuePageBuilder.getSizeInBytes();
    }

    public void add(Block key, Block value, int keyPosition, int valuePosition)
    {
        Page page = new Page(key);
        if (!keysHash.contains(keyPosition, page)) {
            int groupId = keysHash.putIfAbsent(keyPosition, page);
            keysHash.appendValuesTo(groupId, keyPageBuilder, 0);
            if (value.isNull(valuePosition)) {
                valuePageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                valueType.appendTo(value, valuePosition, valuePageBuilder.getBlockBuilder(0));
            }
        }
    }
}
