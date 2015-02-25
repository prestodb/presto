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
import com.facebook.presto.server.SliceSerializer;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.type.MapType.toStackRepresentation;
import static com.facebook.presto.type.TypeJsonUtils.getValue;
import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.google.common.base.Preconditions.checkNotNull;

public class KeyValuePairs
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KeyValuePairs.class).instanceSize();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
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
        Map<Object, Object> map = (Map<Object, Object>) stackRepresentationToObject(null, serialized, new MapType(keyType, valueType));
        for (Map.Entry<Object, Object> entry : map.entrySet()) {
            add(createBlock(entry.getKey(), keyType), createBlock(entry.getValue(), valueType), 0);
        }
    }

    // Once we move to Page for the native container type for Maps we will get rid of the auto-boxing/unboxing here
    public Slice serialize()
    {
        Map<Object, Object> newMap = new LinkedHashMap<>();
        Block values = valuePageBuilder.getBlockBuilder(0).build();
        Block keys = keyPageBuilder.getBlockBuilder(0).build();
        for (int i = 0; i < keys.getPositionCount(); i++) {
            newMap.put(getValue(keys, keyType, i), getValue(values, valueType, i));
        }
        return toStackRepresentation(newMap);
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

    private static Block createBlock(Object obj, Type type)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        try {
            if (obj == null) {
                return blockBuilder.appendNull().build();
            }
            else if (type.getJavaType() == double.class) {
                type.writeDouble(blockBuilder, ((Number) obj).doubleValue());
            }
            else if (type.getJavaType() == long.class) {
                type.writeLong(blockBuilder, ((Number) obj).longValue());
            }
            else if (type.getJavaType() == Slice.class) {
                //TODO is there a simpler way to handle these types?
                if (type instanceof VarcharType) {
                    type.writeSlice(blockBuilder, Slices.utf8Slice((String) obj));
                }
                else if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
                    type.writeSlice(blockBuilder, Slices.utf8Slice(OBJECT_MAPPER.writeValueAsString(obj)));
                }
                else {
                    type.writeSlice(blockBuilder, (Slice) obj);
                }
            }
            else if (type.getJavaType() == boolean.class) {
                type.writeBoolean(blockBuilder, (Boolean) obj);
            }
            else {
                throw new IllegalArgumentException("Unsupported type: " + type.getJavaType().getSimpleName());
            }
        }
        catch (IOException ioe) {
            Throwables.propagate(ioe);
        }

        return blockBuilder.build();
    }
}
