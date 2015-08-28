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

import com.facebook.presto.operator.HashGenerator;
import com.facebook.presto.operator.InterpretedHashGenerator;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.block.VariableWidthBlockEncoding;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeLiteralCalculation;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;

public final class TypeUtils
{
    public static final int EXPECTED_ARRAY_SIZE = 1024;
    public static final int NULL_HASH_CODE = 0;

    private TypeUtils()
    {
    }

    public static int expectedValueSize(Type type, int defaultSize)
    {
        if (type instanceof FixedWidthType) {
            return ((FixedWidthType) type).getFixedSize();
        }
        return defaultSize;
    }

    public static int hashPosition(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        return type.hash(block, position);
    }

    public static long hashPosition(MethodHandle methodHandle, Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return NULL_HASH_CODE;
        }
        try {
            if (type.getJavaType() == boolean.class) {
                return (long) methodHandle.invoke(type.getBoolean(block, position));
            }
            else if (type.getJavaType() == long.class) {
                return (long) methodHandle.invoke(type.getLong(block, position));
            }
            else if (type.getJavaType() == double.class) {
                return (long) methodHandle.invoke(type.getDouble(block, position));
            }
            else if (type.getJavaType() == Slice.class) {
                return (long) methodHandle.invoke(type.getSlice(block, position));
            }
            else {
                throw new UnsupportedOperationException("Unsupported native container type: " + type.getJavaType() + " with type " + type.getTypeSignature());
            }
        }
        catch (Throwable throwable) {
            throw Throwables.propagate(throwable);
        }
    }

    public static boolean positionEqualsPosition(Type type, Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        boolean leftIsNull = leftBlock.isNull(leftPosition);
        boolean rightIsNull = rightBlock.isNull(rightPosition);
        if (leftIsNull || rightIsNull) {
            return leftIsNull && rightIsNull;
        }
        return type.equalTo(leftBlock, leftPosition, rightBlock, rightPosition);
    }

    public static Type resolveType(TypeSignature typeName, TypeManager typeManager)
    {
        return checkNotNull(typeManager.getType(typeName), "Type '%s' not found", typeName);
    }

    public static List<Type> resolveTypes(List<TypeSignature> typeNames, TypeManager typeManager)
    {
        return typeNames.stream()
                .map((TypeSignature type) -> resolveType(type, typeManager))
                .collect(toImmutableList());
    }

    public static TypeSignature parameterizedTypeName(String base, TypeSignature... argumentNames)
    {
        return new TypeSignature(base, ImmutableList.copyOf(argumentNames), ImmutableList.of());
    }

    public static int getHashPosition(List<? extends Type> hashTypes, Block[] hashBlocks, int position)
    {
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        Page page = new Page(hashBlocks);
        return hashGenerator.hashPosition(position, page);
    }

    public static Block getHashBlock(List<? extends Type> hashTypes, Block... hashBlocks)
    {
        checkArgument(hashTypes.size() == hashBlocks.length);
        int[] hashChannels = new int[hashBlocks.length];
        for (int i = 0; i < hashBlocks.length; i++) {
            hashChannels[i] = i;
        }
        HashGenerator hashGenerator = new InterpretedHashGenerator(ImmutableList.copyOf(hashTypes), hashChannels);
        int positionCount = hashBlocks[0].getPositionCount();
        BlockBuilder builder = BIGINT.createFixedSizeBlockBuilder(positionCount);
        Page page = new Page(hashBlocks);
        for (int i = 0; i < positionCount; i++) {
            BIGINT.writeLong(builder, hashGenerator.hashPosition(i, page));
        }
        return builder.build();
    }

    public static Page getHashPage(Page page, List<? extends Type> types, List<Integer> hashChannels)
    {
        Block[] blocks = Arrays.copyOf(page.getBlocks(), page.getChannelCount() + 1);
        ImmutableList.Builder<Type> hashTypes = ImmutableList.builder();
        Block[] hashBlocks = new Block[hashChannels.size()];
        int hashBlockIndex = 0;

        for (int channel : hashChannels) {
            hashTypes.add(types.get(channel));
            hashBlocks[hashBlockIndex++] = blocks[channel];
        }
        blocks[page.getChannelCount()] = getHashBlock(hashTypes.build(), hashBlocks);
        return new Page(blocks);
    }

    public static Block createBlock(Type type, Object element)
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), 1, EXPECTED_ARRAY_SIZE);
        appendToBlockBuilder(type, element, blockBuilder);
        return blockBuilder.build();
    }

    public static void appendToBlockBuilder(Type type, Object element, BlockBuilder blockBuilder)
    {
        Class<?> javaType = type.getJavaType();
        if (element == null) {
            blockBuilder.appendNull();
        }
        // TODO: This should be removed. Functions that rely on this functionality should
        // be doing the conversion themselves.
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ARRAY) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), EXPECTED_ARRAY_SIZE);
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(0), subElement, subBlockBuilder);
            }
            type.writeSlice(blockBuilder, buildStructuralSlice(subBlockBuilder));
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.ROW) && element instanceof Iterable<?>) {
            BlockBuilder subBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), EXPECTED_ARRAY_SIZE);
            int field = 0;
            for (Object subElement : (Iterable<?>) element) {
                appendToBlockBuilder(type.getTypeParameters().get(field), subElement, subBlockBuilder);
                field++;
            }
            type.writeSlice(blockBuilder, buildStructuralSlice(subBlockBuilder));
        }
        else if (type.getTypeSignature().getBase().equals(StandardTypes.MAP) && element instanceof Map<?, ?>) {
            BlockBuilder subBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), EXPECTED_ARRAY_SIZE);
            for (Map.Entry<?, ?> entry : ((Map<?, ?>) element).entrySet()) {
                appendToBlockBuilder(type.getTypeParameters().get(0), entry.getKey(), subBlockBuilder);
                appendToBlockBuilder(type.getTypeParameters().get(1), entry.getValue(), subBlockBuilder);
            }
            type.writeSlice(blockBuilder, buildStructuralSlice(subBlockBuilder));
        }

        else if (javaType == boolean.class) {
            type.writeBoolean(blockBuilder, (Boolean) element);
        }
        else if (javaType == long.class) {
            type.writeLong(blockBuilder, ((Number) element).longValue());
        }
        else if (javaType == double.class) {
            type.writeDouble(blockBuilder, ((Number) element).doubleValue());
        }
        else if (javaType == Slice.class) {
            if (element instanceof String) {
                type.writeSlice(blockBuilder, Slices.utf8Slice(element.toString()));
            }
            else if (element instanceof byte[]) {
                type.writeSlice(blockBuilder, Slices.wrappedBuffer((byte[]) element));
            }
            else {
                type.writeSlice(blockBuilder, (Slice) element);
            }
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format("Unexpected type %s", javaType.getName()));
        }
    }

    public static Object castValue(Type type, Block block, int position)
    {
        Class<?> javaType = type.getJavaType();

        if (block.isNull(position)) {
            return null;
        }
        else if (javaType == boolean.class) {
            return type.getBoolean(block, position);
        }
        else if (javaType == long.class) {
            return type.getLong(block, position);
        }
        else if (javaType == double.class) {
            return type.getDouble(block, position);
        }
        else if (type.getJavaType() == Slice.class) {
            return type.getSlice(block, position);
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, String.format("Unexpected type %s", javaType.getName()));
        }
    }

    public static Slice buildStructuralSlice(BlockBuilder builder)
    {
        return buildStructuralSlice(builder.build());
    }

    public static Slice buildStructuralSlice(Block block)
    {
        BlockEncoding encoding = block.getEncoding();
        DynamicSliceOutput output = new DynamicSliceOutput(encoding.getEstimatedSize(block));
        encoding.writeBlock(output, block);

        return output.slice();
    }

    public static Block readStructuralBlock(Slice slice)
    {
        return new VariableWidthBlockEncoding().readBlock(slice.getInput());
    }

    public static void checkElementNotNull(boolean isNull, String errorMsg)
    {
        if (isNull) {
            throw new PrestoException(NOT_SUPPORTED, errorMsg);
        }
    }

    public static TypeSignature resolveCalculatedType(TypeSignature typeSignature, Map<String, OptionalLong> inputs)
    {
        List<TypeSignature> parameters = typeSignature.getParameters().stream()
                .map(parameter -> resolveCalculatedType(parameter, inputs))
                .collect(toList());

        List<Object> literalParameters = typeSignature.getLiteralParameters().stream()
                .map(literal -> calculateLiteral(literal, inputs))
                .map(literal -> {
                    if (!(literal instanceof OptionalLong)) {
                        return literal;
                    }
                    OptionalLong optionalLong = (OptionalLong) literal;
                    return optionalLong.isPresent() ? optionalLong.getAsLong() : null;
                })
                .collect(toList());

        if (literalParameters.stream().anyMatch(Objects::isNull)) {
            // if we have types missing literal parameters (e.g., just VARCHAR), we can not perform
            // the literal calculation, so just return a raw type
            return new TypeSignature(typeSignature.getBase(), parameters, Collections.emptyList());
        }

        return new TypeSignature(typeSignature.getBase(), parameters, literalParameters);
    }

    public static Object calculateLiteral(Object literal, Map<String, OptionalLong> inputs)
    {
        if (!(literal instanceof TypeLiteralCalculation)) {
            return literal;
        }
        TypeLiteralCalculation typeLiteralCalculation = (TypeLiteralCalculation) literal;
        return TypeCalculation.calculateLiteralValue(typeLiteralCalculation.getCalculation(), inputs);
    }

    public static Map<String, OptionalLong> extractLiteralParameters(TypeSignature typeSignature, TypeSignature actualType)
    {
        if (!typeSignature.isCalculated()) {
            return emptyMap();
        }
        Map<String, OptionalLong> inputs = new HashMap<>();
        for (int index = 0; index < typeSignature.getParameters().size(); index++) {
            TypeSignature parameter = typeSignature.getParameters().get(index);
            if (parameter.isCalculated()) {
                TypeSignature actualParameter = actualType.getParameters().get(index);
                inputs.putAll(extractLiteralParameters(parameter, actualParameter));
            }
        }
        for (int index = 0; index < typeSignature.getLiteralParameters().size(); index++) {
            Object literal = typeSignature.getLiteralParameters().get(index);
            if (literal instanceof TypeLiteralCalculation) {
                TypeLiteralCalculation calculation = (TypeLiteralCalculation) literal;
                if (actualType.getLiteralParameters().isEmpty()) {
                    inputs.put(calculation.getCalculation().toUpperCase(Locale.US), OptionalLong.empty());
                }
                else {
                    Object actualLiteral = actualType.getLiteralParameters().get(index);
                    if (!(actualLiteral instanceof Long)) {
                        throw new IllegalArgumentException(format("Expected type %s literal parameter %s to be a number", actualType, index));
                    }
                    inputs.put(calculation.getCalculation().toUpperCase(Locale.US), OptionalLong.of((Long) actualLiteral));
                }
            }
        }
        return inputs;
    }
}
