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
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.function.SqlFunctionProperties;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.InvocationConvention.simpleConvention;
import static com.facebook.presto.common.type.TypeUtils.NULL_HASH_CODE;
import static com.facebook.presto.common.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.common.type.TypeUtils.hashPosition;
import static java.lang.String.format;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

public class MapType
        extends AbstractType
{
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, NULL_FLAG);

    private static final MethodHandle EQUAL;
    private static final MethodHandle HASH_CODE;

    private static final MethodHandle SEEK_KEY;
    private static final MethodHandle DISTINCT_FROM;
    private static final MethodHandle INDETERMINATE;

    static {
        try {
            Lookup lookup = MethodHandles.lookup();
            EQUAL = lookup.findStatic(MapType.class, "equalOperator", methodType(Boolean.class, MethodHandle.class, MethodHandle.class, Block.class, Block.class));
            HASH_CODE = lookup.findStatic(MapType.class, "hashOperator", methodType(long.class, MethodHandle.class, MethodHandle.class, Block.class));
            DISTINCT_FROM = lookup.findStatic(MapType.class, "distinctFromOperator", methodType(boolean.class, MethodHandle.class, MethodHandle.class, Block.class, Block.class));
            INDETERMINATE = lookup.findStatic(MapType.class, "indeterminate", methodType(boolean.class, MethodHandle.class, Block.class, boolean.class));
            SEEK_KEY = lookup.findVirtual(
                    SingleMapBlock.class,
                    "seekKey",
                    methodType(int.class, MethodHandle.class, MethodHandle.class, Block.class, int.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

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

    private static long hashOperator(MethodHandle keyOperator, MethodHandle valueOperator, Block block)
            throws Throwable
    {
        long result = 0;
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            result += invokeHashOperator(keyOperator, block, i) ^ invokeHashOperator(valueOperator, block, i + 1);
        }
        return result;
    }

    private static long invokeHashOperator(MethodHandle keyOperator, Block block, int position)
            throws Throwable
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return (long) keyOperator.invokeExact(block, position);
    }

    private static Boolean equalOperator(
            MethodHandle seekKey,
            MethodHandle valueEqualOperator,
            Block leftBlock,
            Block rightBlock)
            throws Throwable
    {
        if (leftBlock.getPositionCount() != rightBlock.getPositionCount()) {
            return false;
        }

        SingleMapBlock leftSingleMapLeftBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightSingleMapBlock = (SingleMapBlock) rightBlock;

        boolean unknown = false;
        for (int position = 0; position < leftSingleMapLeftBlock.getPositionCount(); position += 2) {
            int leftPosition = position + 1;
            int rightPosition = (int) seekKey.invokeExact(rightSingleMapBlock, leftBlock, position);
            if (rightPosition == -1) {
                return false;
            }

            if (leftBlock.isNull(leftPosition) || rightBlock.isNull(rightPosition)) {
                unknown = true;
            }
            else {
                Boolean result = (Boolean) valueEqualOperator.invokeExact((Block) leftSingleMapLeftBlock, leftPosition, (Block) rightSingleMapBlock, rightPosition);
                if (result == null) {
                    unknown = true;
                }
                else if (!result) {
                    return false;
                }
            }
        }

        if (unknown) {
            return null;
        }
        return true;
    }

    private static boolean distinctFromOperator(
            MethodHandle seekKey,
            MethodHandle valueDistinctFromOperator,
            Block leftBlock,
            Block rightBlock)
            throws Throwable
    {
        boolean leftIsNull = leftBlock == null;
        boolean rightIsNull = rightBlock == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }

        if (leftBlock.getPositionCount() != rightBlock.getPositionCount()) {
            return true;
        }

        SingleMapBlock leftSingleMapLeftBlock = (SingleMapBlock) leftBlock;
        SingleMapBlock rightSingleMapBlock = (SingleMapBlock) rightBlock;

        for (int position = 0; position < leftSingleMapLeftBlock.getPositionCount(); position += 2) {
            int leftPosition = position + 1;
            int rightPosition = (int) seekKey.invokeExact(rightSingleMapBlock, leftBlock, position);
            if (rightPosition == -1) {
                return true;
            }

            boolean result = (boolean) valueDistinctFromOperator.invokeExact((Block) leftSingleMapLeftBlock, leftPosition, (Block) rightSingleMapBlock, rightPosition);
            if (result) {
                return true;
            }
        }

        return false;
    }

    private static boolean indeterminate(MethodHandle valueIndeterminateFunction, Block block, boolean isNull)
            throws Throwable
    {
        if (isNull) {
            return true;
        }
        for (int i = 0; i < block.getPositionCount(); i += 2) {
            // since maps are not allowed to have indeterminate keys we only check values here
            if (block.isNull(i + 1)) {
                return true;
            }
            if ((boolean) valueIndeterminateFunction.invokeExact(block, i + 1)) {
                return true;
            }
        }
        return false;
    }
}
