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
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.openjdk.jol.info.ClassLayout;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static com.facebook.presto.type.TypeJsonUtils.stackRepresentationToObject;
import static com.facebook.presto.type.TypeUtils.getValue;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.Map.Entry;

public class KeyValuePairs
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(KeyValuePairs.class).instanceSize();
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get().registerModule(new SimpleModule().addSerializer(Slice.class, new SliceSerializer()));
    public static final int EXPECTED_HASH_SIZE = 10_000;

    private GroupByHash keysHash;
    private PageBuilder keyPageBuilder;
    private Type keyType;

    private PageBuilder valuePageBuilder;
    private Type valueType;

    public KeyValuePairs(Type keyType, Type valueType)
    {
        checkNotNull(keyType, "keyType is null");
        checkNotNull(valueType, "valueType is null");

        this.keyType = keyType;
        this.valueType = valueType;
        keysHash = new GroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.<Integer>absent(), EXPECTED_HASH_SIZE);
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
        keysHash = new GroupByHash(ImmutableList.of(keyType), new int[] {0}, Optional.<Integer>absent(), 10_000);
        keyPageBuilder = new PageBuilder(ImmutableList.of(this.keyType));
        valuePageBuilder = new PageBuilder(ImmutableList.of(this.valueType));
        deserialize(serialized);
    }

    //once we move to Page for the native container type for Maps we will get rid of the auto-boxing/unboxing here
    private void deserialize(Slice serialized)
    {
        Map<Object, Object> map = (Map) stackRepresentationToObject(null, serialized, new MapType(keyType, valueType));
        map.entrySet().
                stream().
                map(this::createBlocks).
                forEach(blocks -> add(blocks[0], blocks[1], 0));
    }

    //once we move to Page for the native container type for Maps we will get rid of the auto-boxing/unboxing here
    public Slice serialize()
    {
        Map newMap = new LinkedHashMap<>();
        Block values = valuePageBuilder.getBlockBuilder(0).build();
        Block keys = keyPageBuilder.getBlockBuilder(0).build();
        IntStream.range(0, keys.getPositionCount()).
                mapToObj(position -> new Object[] { getValue(keys, keyType, position), getValue(values, valueType, position) }).
                forEach(x -> newMap.put(x[0], x[1]));
        return MapType.toStackRepresentation(newMap);
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
            keysHash.appendValuesTo(groupId, keyPageBuilder, 0); //TODO outputChannelOffset??
            if (value.isNull(position)) {
                valuePageBuilder.getBlockBuilder(0).appendNull();
            }
            else {
                valueType.appendTo(value, position, valuePageBuilder.getBlockBuilder(0)); //TODO outputChannelOffset??
            }
        }
    }

    private Block[] createBlocks(Entry entry)
    {
        return new Block[] { createBlock(entry.getKey(), keyType), createBlock(entry.getValue(), valueType) };
    }

    private Block createBlock(Object obj, Type type)
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
