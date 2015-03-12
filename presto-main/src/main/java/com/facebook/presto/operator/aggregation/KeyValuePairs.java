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
import com.facebook.presto.spi.block.StructBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.openjdk.jol.info.ClassLayout;

import java.util.Optional;

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
        keysHash = new GroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.empty(), EXPECTED_HASH_SIZE);
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
        keysHash = new GroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.empty(), 10_000);
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

    // Once we move to Page for the native container type for Maps we will get rid of the auto-boxing/unboxing here
    private void deserialize(Slice serialized)
    {
        Block block = StructBuilder.readBlock(serialized.getInput());
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            add(block.getSingleValueBlock(i), block.getSingleValueBlock(i + 1), 0);
        }
    }

    public Slice serialize()
    {
        StructBuilder builder = StructBuilder.mapBuilder(keyType, valueType);
        Block values = valuePageBuilder.getBlockBuilder(0).build();
        Block keys = keyPageBuilder.getBlockBuilder(0).build();
        for (int i = 0; i < keys.getPositionCount(); i++) {
            builder.add(keys, i);
            builder.add(values, i);
        }
        return builder.build();
    }

    public long estimatedInMemorySize()
    {
        return INSTANCE_SIZE + keyPageBuilder.getSizeInBytes() + valuePageBuilder.getSizeInBytes();
    }

    public void add(Block key, Block value, int position)
    {
        Page page = new Page(key);
        if (!keysHash.contains(position, page)) {
            int groupId = keysHash.putIfAbsent(position, page, new Block[] { key });
            keysHash.appendValuesTo(groupId, keyPageBuilder, 0);
            if (value.isNull(position)) {
                valuePageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                valueType.appendTo(value, position, valuePageBuilder.getBlockBuilder(0));
            }
        }
    }
}
