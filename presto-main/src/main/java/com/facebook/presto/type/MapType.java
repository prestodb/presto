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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeUtils.buildMapSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readMapBlocks;
import static com.google.common.base.Preconditions.checkArgument;

public class MapType
        extends AbstractVariableWidthType
{
    private final Type keyType;
    private final Type valueType;

    public MapType(Type keyType, Type valueType)
    {
        super(parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), Slice.class);
        checkArgument(keyType.isComparable(), "key type must be comparable");
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public static Slice toStackRepresentation(Map<?, ?> value, Type keyType, Type valueType)
    {
        BlockBuilder keyBuilder = keyType.createBlockBuilder(new BlockBuilderStatus(), value.size());
        BlockBuilder valueBuilder = valueType.createBlockBuilder(new BlockBuilderStatus(), value.size());
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), keyBuilder);
            appendToBlockBuilder(valueType, entry.getValue(), valueBuilder);
        }
        return buildMapSlice(keyBuilder, valueBuilder);
    }

    public Type getKeyType()
    {
        return keyType;
    }

    public Type getValueType()
    {
        return valueType;
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getLength(position));
        Block[] mapBlocks = readMapBlocks(keyType, valueType, slice);
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < mapBlocks[0].getPositionCount(); i++) {
            map.put(keyType.getObjectValue(session, mapBlocks[0], i), valueType.getObjectValue(session, mapBlocks[1], i));
        }

        return Collections.unmodifiableMap(map);
    }

    @Override
    public void appendTo(Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            blockBuilder.appendNull();
        }
        else {
            block.writeBytesTo(position, 0, block.getLength(position), blockBuilder);
            blockBuilder.closeEntry();
        }
    }

    @Override
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getLength(position));
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value)
    {
        writeSlice(blockBuilder, value, 0, value.length());
    }

    @Override
    public void writeSlice(BlockBuilder blockBuilder, Slice value, int offset, int length)
    {
        blockBuilder.writeBytes(value, offset, length).closeEntry();
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(getKeyType(), getValueType());
    }
}
