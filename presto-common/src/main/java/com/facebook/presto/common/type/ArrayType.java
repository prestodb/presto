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

import com.facebook.presto.common.block.AbstractArrayBlock;
import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.BlockBuilderStatus;
import com.facebook.presto.common.function.InvocationConvention;
import com.facebook.presto.common.function.OperatorMethodHandle;
import com.facebook.presto.common.function.SqlFunctionProperties;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.InvocationConvention.simpleConvention;
import static com.facebook.presto.common.type.StandardTypes.ARRAY;
import static com.facebook.presto.common.type.TypeUtils.NULL_HASH_CODE;
import static com.facebook.presto.common.type.TypeUtils.checkElementNotNull;
import static com.facebook.presto.common.type.TypeUtils.hashPosition;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class ArrayType
        extends AbstractType
{
    private static final InvocationConvention EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, NEVER_NULL, NEVER_NULL);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL);
    private static final InvocationConvention DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BOXED_NULLABLE, BOXED_NULLABLE);
    private static final InvocationConvention INDETERMINATE_CONVENTION = simpleConvention(FAIL_ON_NULL, NULL_FLAG);
    private static final InvocationConvention COMPARISON_CONVENTION = simpleConvention(FAIL_ON_NULL, NEVER_NULL, NEVER_NULL);

    private static final MethodHandle EQUAL;
    private static final MethodHandle HASH_CODE;
    private static final MethodHandle DISTINCT_FROM;
    private static final MethodHandle INDETERMINATE;
    private static final MethodHandle COMPARISON;

    static {
        try {
            Lookup lookup = MethodHandles.lookup();
            EQUAL = lookup.findStatic(ArrayType.class, "equalOperator", MethodType.methodType(Boolean.class, MethodHandle.class, Block.class, Block.class));
            HASH_CODE = lookup.findStatic(ArrayType.class, "hashOperator", MethodType.methodType(long.class, MethodHandle.class, Block.class));
            DISTINCT_FROM = lookup.findStatic(ArrayType.class, "distinctFromOperator", MethodType.methodType(boolean.class, MethodHandle.class, Block.class, Block.class));
            INDETERMINATE = lookup.findStatic(ArrayType.class, "indeterminateOperator", MethodType.methodType(boolean.class, MethodHandle.class, Block.class, boolean.class));
            COMPARISON = lookup.findStatic(ArrayType.class, "comparisonOperator", MethodType.methodType(long.class, MethodHandle.class, Block.class, Block.class));
        }
        catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    private final Type elementType;
    public static final String ARRAY_NULL_ELEMENT_MSG = "ARRAY comparison not supported for arrays with null elements";

    // this field is used in double checked locking
    @SuppressWarnings("FieldAccessedSynchronizedAndUnsynchronized")
    private volatile TypeOperatorDeclaration operatorDeclaration;

    public ArrayType(Type elementType)
    {
        super(new TypeSignature(ARRAY, TypeSignatureParameter.of(elementType.getTypeSignature())), Block.class);
        this.elementType = requireNonNull(elementType, "elementType is null");
    }

    @Override
    public TypeOperatorDeclaration getTypeOperatorDeclaration(TypeOperators typeOperators)
    {
        if (operatorDeclaration == null) {
            generateTypeOperators(typeOperators);
        }
        return operatorDeclaration;
    }

    private synchronized void generateTypeOperators(TypeOperators typeOperators)
    {
        if (operatorDeclaration != null) {
            return;
        }
        operatorDeclaration = TypeOperatorDeclaration.builder(getJavaType())
                .addEqualOperators(getEqualOperatorMethodHandles(typeOperators, elementType))
                .addHashCodeOperators(getHashCodeOperatorMethodHandles(typeOperators, elementType))
                .addXxHash64Operators(getXxHash64OperatorMethodHandles(typeOperators, elementType))
                .addDistinctFromOperators(getDistinctFromOperatorInvokers(typeOperators, elementType))
                .addIndeterminateOperators(getIndeterminateOperatorInvokers(typeOperators, elementType))
                .addComparisonOperators(getComparisonOperatorInvokers(typeOperators, elementType))
                .build();
    }

    private static List<OperatorMethodHandle> getEqualOperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle equalOperator = typeOperators.getEqualOperator(elementType, simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION));
        return singletonList(new OperatorMethodHandle(EQUAL_CONVENTION, EQUAL.bindTo(equalOperator)));
    }

    private static List<OperatorMethodHandle> getHashCodeOperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementHashCodeOperator = typeOperators.getHashCodeOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getXxHash64OperatorMethodHandles(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementHashCodeOperator = typeOperators.getXxHash64Operator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return singletonList(new OperatorMethodHandle(HASH_CODE_CONVENTION, HASH_CODE.bindTo(elementHashCodeOperator)));
    }

    private static List<OperatorMethodHandle> getDistinctFromOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementDistinctFromOperator = typeOperators.getDistinctFromOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        return singletonList(new OperatorMethodHandle(DISTINCT_FROM_CONVENTION, DISTINCT_FROM.bindTo(elementDistinctFromOperator)));
    }

    private static List<OperatorMethodHandle> getIndeterminateOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isComparable()) {
            return emptyList();
        }
        MethodHandle elementIndeterminateOperator = typeOperators.getIndeterminateOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION));
        return singletonList(new OperatorMethodHandle(INDETERMINATE_CONVENTION, INDETERMINATE.bindTo(elementIndeterminateOperator)));
    }

    private static List<OperatorMethodHandle> getComparisonOperatorInvokers(TypeOperators typeOperators, Type elementType)
    {
        if (!elementType.isOrderable()) {
            return emptyList();
        }
        MethodHandle elementComparisonOperator = typeOperators.getComparisonOperator(elementType, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION));
        return singletonList(new OperatorMethodHandle(COMPARISON_CONVENTION, COMPARISON.bindTo(elementComparisonOperator)));
    }

    public Type getElementType()
    {
        return elementType;
    }

    @Override
    public boolean isComparable()
    {
        return elementType.isComparable();
    }

    @Override
    public boolean isOrderable()
    {
        return elementType.isOrderable();
    }

    @Override
    public boolean equalTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        Block leftArray = leftBlock.getBlock(leftPosition);
        Block rightArray = rightBlock.getBlock(rightPosition);

        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        for (int i = 0; i < leftArray.getPositionCount(); i++) {
            checkElementNotNull(leftArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(i), ARRAY_NULL_ELEMENT_MSG);
            if (!elementType.equalTo(leftArray, i, rightArray, i)) {
                return false;
            }
        }

        return true;
    }

    @Override
    public long hash(Block block, int position)
    {
        Block array = getObject(block, position);
        long hash = 0;
        for (int i = 0; i < array.getPositionCount(); i++) {
            hash = 31 * hash + hashPosition(elementType, array, i);
        }
        return hash;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        if (!elementType.isOrderable()) {
            throw new UnsupportedOperationException(getTypeSignature() + " type is not orderable");
        }

        Block leftArray = leftBlock.getBlock(leftPosition);
        Block rightArray = rightBlock.getBlock(rightPosition);

        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        int index = 0;
        while (index < len) {
            checkElementNotNull(leftArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(index), ARRAY_NULL_ELEMENT_MSG);
            int comparison = elementType.compareTo(leftArray, index, rightArray, index);
            if (comparison != 0) {
                return comparison;
            }
            index++;
        }

        if (index == len) {
            return leftArray.getPositionCount() - rightArray.getPositionCount();
        }

        return 0;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        if (block instanceof AbstractArrayBlock) {
            return ((AbstractArrayBlock) block).apply((valuesBlock, start, length) -> arrayBlockToObjectValues(properties, valuesBlock, start, length), position);
        }
        else {
            Block arrayBlock = block.getBlock(position);
            return arrayBlockToObjectValues(properties, arrayBlock, 0, arrayBlock.getPositionCount());
        }
    }

    private List<Object> arrayBlockToObjectValues(SqlFunctionProperties properties, Block block, int start, int length)
    {
        List<Object> values = new ArrayList<>(length);

        for (int i = 0; i < length; i++) {
            values.add(elementType.getObjectValue(properties, block, i + start));
        }

        return Collections.unmodifiableList(values);
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
    public Slice getSlice(Block block, int position)
    {
        return block.getSlice(position, 0, block.getSliceLength(position));
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
        blockBuilder.appendStructure((Block) value);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries, int expectedBytesPerEntry)
    {
        return new ArrayBlockBuilder(elementType, blockBuilderStatus, expectedEntries, expectedBytesPerEntry);
    }

    @Override
    public BlockBuilder createBlockBuilder(BlockBuilderStatus blockBuilderStatus, int expectedEntries)
    {
        return createBlockBuilder(blockBuilderStatus, expectedEntries, 100);
    }

    @Override
    public List<Type> getTypeParameters()
    {
        return singletonList(getElementType());
    }

    @Override
    public String getDisplayName()
    {
        return ARRAY + "(" + elementType.getDisplayName() + ")";
    }

    private static Boolean equalOperator(MethodHandle equalOperator, Block leftArray, Block rightArray)
            throws Throwable
    {
        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return false;
        }

        boolean unknown = false;
        for (int position = 0; position < leftArray.getPositionCount(); position++) {
            if (leftArray.isNull(position) || rightArray.isNull(position)) {
                unknown = true;
                continue;
            }
            Boolean result = (Boolean) equalOperator.invokeExact(leftArray, position, rightArray, position);
            if (result == null) {
                unknown = true;
            }
            else if (!result) {
                return false;
            }
        }

        if (unknown) {
            return null;
        }
        return true;
    }

    private static long hashOperator(MethodHandle hashOperator, Block block)
            throws Throwable
    {
        long hash = 0;
        for (int position = 0; position < block.getPositionCount(); position++) {
            long elementHash = block.isNull(position) ? NULL_HASH_CODE : (long) hashOperator.invokeExact(block, position);
            hash = 31 * hash + elementHash;
        }
        return hash;
    }

    private static boolean distinctFromOperator(MethodHandle distinctFromOperator, Block leftArray, Block rightArray)
            throws Throwable
    {
        boolean leftIsNull = leftArray == null;
        boolean rightIsNull = rightArray == null;
        if (leftIsNull || rightIsNull) {
            return leftIsNull != rightIsNull;
        }

        if (leftArray.getPositionCount() != rightArray.getPositionCount()) {
            return true;
        }

        for (int position = 0; position < leftArray.getPositionCount(); position++) {
            boolean result = (boolean) distinctFromOperator.invokeExact(leftArray, position, rightArray, position);
            if (result) {
                return true;
            }
        }

        return false;
    }

    private static boolean indeterminateOperator(MethodHandle elementIndeterminateFunction, Block block, boolean isNull)
            throws Throwable
    {
        if (isNull) {
            return true;
        }

        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                return true;
            }
            if ((boolean) elementIndeterminateFunction.invoke(block, position)) {
                return true;
            }
        }
        return false;
    }

    private static long comparisonOperator(MethodHandle comparisonOperator, Block leftArray, Block rightArray)
            throws Throwable
    {
        int len = Math.min(leftArray.getPositionCount(), rightArray.getPositionCount());
        for (int position = 0; position < len; position++) {
            checkElementNotNull(leftArray.isNull(position), ARRAY_NULL_ELEMENT_MSG);
            checkElementNotNull(rightArray.isNull(position), ARRAY_NULL_ELEMENT_MSG);

            long result = (long) comparisonOperator.invokeExact(leftArray, position, rightArray, position);
            if (result != 0) {
                return result;
            }
        }

        return Integer.compare(leftArray.getPositionCount(), rightArray.getPositionCount());
    }
}
