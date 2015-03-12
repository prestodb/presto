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
import com.facebook.presto.spi.block.StructBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceInput;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.block.StructBuilder.readBlock;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;

public class MapType
        extends AbstractVariableWidthType
{
    private final Type keyType;
    private final Type valueType;

    public MapType(Type keyType, Type valueType)
    {
        super(parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), Slice.class);
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public static Slice toStackRepresentation(Map<?, ?> value, Type keyType, Type valueType)
    {
        StructBuilder builder = StructBuilder.mapBuilder(keyType, valueType);
        value.forEach((k, v) -> builder.add(k).add(v));

        return builder.build();
    }

    public static <T, S> Map<T, S> stackRepresentationToObject(ConnectorSession session, SliceInput input, Type keyType, Type valueType)
    {
        Block block = readBlock(input);
        HashMap<T, S> map = new HashMap<>();
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            map.put((T) keyType.getObjectValue(session, block, i), (S) valueType.getObjectValue(session, block, i + 1));
        }

        return Collections.unmodifiableMap(map);
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
        return stackRepresentationToObject(session, slice.getInput(), keyType, valueType);
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
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus)
    {
        return new VariableWidthBlockBuilder(blockBuilderStatus);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return ImmutableList.of(getKeyType(), getValueType());
    }
}
