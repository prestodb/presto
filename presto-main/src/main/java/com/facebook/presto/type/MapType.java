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
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractVariableWidthType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.type.TypeUtils.appendToBlockBuilder;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.type.TypeUtils.hashPosition;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.google.common.base.Preconditions.checkArgument;

public class MapType
        extends AbstractVariableWidthType
{
    private final Type keyType;
    private final Type valueType;
    private static final String MAP_NULL_ELEMENT_MSG = "MAP comparison not supported for null value elements";

    public MapType(Type keyType, Type valueType)
    {
        super(parameterizedTypeName("map", keyType.getTypeSignature(), valueType.getTypeSignature()), Slice.class);
        checkArgument(keyType.isComparable(), "key type must be comparable");
        this.keyType = keyType;
        this.valueType = valueType;
    }

    public static Slice toStackRepresentation(Map<?, ?> value, Type keyType, Type valueType)
    {
        BlockBuilder blockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), 1024);
        for (Map.Entry<?, ?> entry : value.entrySet()) {
            appendToBlockBuilder(keyType, entry.getKey(), blockBuilder);
            appendToBlockBuilder(valueType, entry.getValue(), blockBuilder);
        }
        return buildStructuralSlice(blockBuilder);
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
    public boolean isComparable()
    {
        return valueType.isComparable();
    }

    @Override
    public int hash(Block block, int position)
    {
        Slice mapSlice = getSlice(block, position);
        Block mapBlock = readStructuralBlock(mapSlice);
        int result = 0;

        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            result += hashPosition(keyType, mapBlock, i);
            result += hashPosition(valueType, mapBlock, i + 1);
        }
        return result;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Slice leftSlice = getSlice(leftBlock, leftPosition);
        Slice rightSlice = getSlice(rightBlock, rightPosition);
        Block leftMapBlock = readStructuralBlock(leftSlice);
        Block rightMapBlock = readStructuralBlock(rightSlice);

        if (leftMapBlock.getPositionCount() != rightMapBlock.getPositionCount()) {
            return false;
        }

        Map<KeyWrapper, Integer> wrappedLeftMap = new HashMap<>();
        for (int position = 0; position < leftMapBlock.getPositionCount(); position += 2) {
            wrappedLeftMap.put(new KeyWrapper(keyType, leftMapBlock, position), position + 1);
        }

        for (int position = 0; position < rightMapBlock.getPositionCount(); position += 2) {
            KeyWrapper key = new KeyWrapper(keyType, rightMapBlock, position);
            Integer leftValuePosition = wrappedLeftMap.get(key);
            if (leftValuePosition == null) {
                return false;
            }
            int rightValuePosition = position + 1;
            checkElementNotNull(leftMapBlock.isNull(leftValuePosition), MAP_NULL_ELEMENT_MSG);
            checkElementNotNull(rightMapBlock.isNull(rightValuePosition), MAP_NULL_ELEMENT_MSG);

            if (!valueType.equalTo(leftMapBlock, leftValuePosition, rightMapBlock, rightValuePosition)) {
                return false;
            }
        }
        return true;
    }

    private static final class KeyWrapper
    {
        private final Type type;
        private final Block block;
        private final int position;

        public KeyWrapper(Type type, Block block, int position)
        {
            this.type = type;
            this.block = block;
            this.position = position;
        }

        public Block getBlock()
        {
            return this.block;
        }

        public int getPosition()
        {
            return this.position;
        }

        @Override
        public int hashCode()
        {
            return type.hash(block, position);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null || !getClass().equals(obj.getClass())) {
                return false;
            }
            KeyWrapper other = (KeyWrapper) obj;
            return type.equalTo(this.block, this.position, other.getBlock(), other.getPosition());
        }
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Slice slice = block.getSlice(position, 0, block.getLength(position));
        Block mapBlock = readStructuralBlock(slice);
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            map.put(keyType.getObjectValue(session, mapBlock, i), valueType.getObjectValue(session, mapBlock, i + 1));
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

    @Override
    public String getDisplayName()
    {
        return "map<" + keyType.getDisplayName() + ", " + valueType.getDisplayName() + ">";
    }
}
