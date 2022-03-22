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
package com.facebook.presto.common.type;

import com.facebook.presto.common.block.AbstractMapBlock.HashTables;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.block.MapBlock;
import com.facebook.presto.common.block.MapBlockBuilder;
import com.facebook.presto.common.block.SingleMapBlock;
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.lang.invoke.MethodHandle;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.common.type.TypeUtils.hashPosition;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class MapType
        extends AbstractType
{
    private final Type keyType;
    private final Type valueType;
    private static final String MAP_NULL_ELEMENT_MSG = "MAP comparison not supported for null value elements";
    private static final int EXPECTED_BYTES_PER_ENTRY = 32;

    private final MethodHandle keyBlockHashCode;
    private final MethodHandle keyBlockEquals;

    public MapType(
            Type keyType,
            Type valueType,
            MethodHandle keyBlockEquals,
            MethodHandle keyBlockHashCode)
    {
        super(new TypeSignature(StandardTypes.MAP,
                        TypeSignatureParameter.of(keyType.getTypeSignature()),
                        TypeSignatureParameter.of(valueType.getTypeSignature())),
                Block.class);
        if (!keyType.isComparable()) {
            throw new IllegalArgumentException(format("key type must be comparable, got %s", keyType));
        }
        this.keyType = keyType;
        this.valueType = valueType;
        requireNonNull(keyBlockHashCode, "keyBlockHashCode is null");
        this.keyBlockHashCode = keyBlockHashCode;
        this.keyBlockEquals = keyBlockEquals;
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new MapBlockBuilder(
                keyType,
                valueType,
                keyBlockEquals,
                keyBlockHashCode,
                blockBuilderStatus,
                expectedEntries);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, EXPECTED_BYTES_PER_ENTRY);
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
    public long hash(Block block, int position)
    {
        Block mapBlock = getObject(block, position);
        long result = 0;

        for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
            result += hashPosition(keyType, mapBlock, i) ^ hashPosition(valueType, mapBlock, i + 1);
        }
        return result;
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Block leftMapBlock = leftBlock.getBlock(leftPosition);
        Block rightMapBlock = rightBlock.getBlock(rightPosition);

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
            return Long.hashCode(type.hash(block, position));
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
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        Block singleMapBlock = block.getBlock(position);
        if (!(singleMapBlock instanceof SingleMapBlock)) {
            throw new UnsupportedOperationException("Map is encoded with legacy block representation");
        }
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < singleMapBlock.getPositionCount(); i += 2) {
            map.put(keyType.getObjectValue(properties, singleMapBlock, i), valueType.getObjectValue(properties, singleMapBlock, i + 1));
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
            block.writePositionTo(position, blockBuilder);
        }
    }

    @Override
    public Block getObject(Block block, int position)
    {
        return block.getBlock(position);
    }

    @Override
    public Block getBlockUnchecked(Block block, int internalPosition)
    {
        return block.getBlockUnchecked(internalPosition);
    }

    @Override
    public void writeObject(BlockBuilder blockBuilder, Object value)
    {
        if (!(value instanceof SingleMapBlock)) {
            throw new IllegalArgumentException("Maps must be represented with SingleMapBlock");
        }
        blockBuilder.appendStructure((Block) value);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return asList(getKeyType(), getValueType());
    }

    @Override
    public String getDisplayName()
    {
        return "map(" + keyType.getDisplayName() + ", " + valueType.getDisplayName() + ")";
    }

    public Block createBlockFromKeyValue(int positionCount, Optional<boolean[]> mapIsNull, int[] offsets, Block keyBlock, Block valueBlock)
    {
        return MapBlock.fromKeyValueBlock(
                positionCount,
                mapIsNull,
                offsets,
                keyBlock,
                valueBlock);
    }

    /**
     * Create a map block directly without per element validations.
     * <p>
     * Internal use by com.facebook.presto.spi.Block only.
     */
    public static Block createMapBlockInternal(
            int startOffset,
            int positionCount,
            Optional<boolean[]> mapIsNull,
            int[] offsets,
            Block keyBlock,
            Block valueBlock,
            HashTables hashTables)
    {
        // TypeManager caches types. Therefore, it is important that we go through it instead of coming up with the MethodHandles directly.
        // BIGINT is chosen arbitrarily here. Any type will do.
        return MapBlock.createMapBlockInternal(startOffset, positionCount, mapIsNull, offsets, keyBlock, valueBlock, hashTables);
    }
}
