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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.VariableWidthBlockBuilder;
import com.facebook.presto.spi.type.AbstractFixedWidthType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.google.common.collect.ImmutableList;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.metadata.Signature.comparableTypeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.type.TypeUtils.buildStructuralSlice;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;

public final class ArrayDistinctFunction
        extends ParametricScalar
{
    public static final ArrayDistinctFunction ARRAY_DISTINCT_FUNCTION = new ArrayDistinctFunction();
    private static final String FUNCTION_NAME = "array_distinct";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(comparableTypeParameter("E")), "array<E>", ImmutableList.of("array<E>"), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayDistinctFunction.class, "distinct", Type.class, Slice.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();
    private static final CollectionType COLLECTION_TYPE = OBJECT_MAPPER.getTypeFactory().constructCollectionType(List.class, Object.class);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return false;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Remove duplicate values from the given array";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, format("%s expects only one argument", FUNCTION_NAME));
        Type type = types.get("E");
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(type);
        Signature signature = new Signature(FUNCTION_NAME,
                parameterizedTypeName("array", type.getTypeSignature()),
                parameterizedTypeName("array", type.getTypeSignature()));
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    private static Block listToBlock(final Type type, List<Object> elements)
    {
        BlockBuilder elementsBlockBuilder;
        if (type instanceof AbstractFixedWidthType) {
            elementsBlockBuilder = ((AbstractFixedWidthType) type).createFixedSizeBlockBuilder(elements.size());
        }
        else {
            elementsBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), elements.size());
        }

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            for (Object e : elements) {
                if (e == null) {
                    elementsBlockBuilder.appendNull();
                }
                else {
                    type.writeBoolean(elementsBlockBuilder, (boolean) e);
                }
            }
        }
        else if (javaType == long.class) {
            for (Object e : elements) {
                if (e == null) {
                    elementsBlockBuilder.appendNull();
                }
                else {
                    type.writeLong(elementsBlockBuilder, ((Number) e).longValue());
                }
            }
        }
        else if (javaType == double.class) {
            for (Object e : elements) {
                if (e == null) {
                    elementsBlockBuilder.appendNull();
                }
                else {
                    type.writeDouble(elementsBlockBuilder, (double) e);
                }
            }
        }
        else if (javaType == Slice.class) {
            for (Object e : elements) {
                if (e == null) {
                    elementsBlockBuilder.appendNull();
                }
                else {
                    type.writeSlice(elementsBlockBuilder, utf8Slice((String) e));
                }
            }
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected type %s", javaType.getName()));
        }

        return elementsBlockBuilder.build();
    }

    private static List<Object> blockToList(final Type type, Block block)
    {
        int positionCount = block.getPositionCount();
        ArrayList<Object> result = new ArrayList<>(positionCount);

        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    result.add(null);
                }
                else {
                    result.add(type.getBoolean(block, i));
                }
            }
        }
        else if (javaType == long.class) {
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    result.add(null);
                }
                else {
                    result.add(type.getLong(block, i));
                }
            }
        }
        else if (javaType == double.class) {
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    result.add(null);
                }
                else {
                    result.add(type.getDouble(block, i));
                }
            }
        }
        else if (javaType == Slice.class) {
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    result.add(null);
                }
                else {
                    result.add(type.getSlice(block, i));
                }
            }
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected type %s", javaType.getName()));
        }

        return result;
    }

    public static Slice distinct(final Type type, Slice jsonArray)
    {
        Block elementsBlock = readStructuralBlock(jsonArray);

        if (elementsBlock.getPositionCount() == 0) {
            return jsonArray;
        }

        GroupByHash groupByHash = new GroupByHash(ImmutableList.of(type), new int[] {0}, Optional.empty(), elementsBlock.getPositionCount());
        groupByHash.getGroupIds(new Page(elementsBlock));

        PageBuilder pageBuilder = new PageBuilder(groupByHash.getTypes());
        for (int i = 0; i < groupByHash.getGroupCount(); i++) {
            pageBuilder.declarePosition();
            groupByHash.appendValuesTo(i, pageBuilder, 0);
        }
        BlockBuilder fixedWidthResultBlockBuilder = pageBuilder.getBlockBuilder(0);
        Block fixedWidthResultBlock = fixedWidthResultBlockBuilder.build();

        // Convert the fixed width block to a variable width block because of the limitations of array representation
        BlockBuilder variableWidthBlockBuilder = new VariableWidthBlockBuilder(new BlockBuilderStatus(), fixedWidthResultBlock.getSizeInBytes());
        for (int i = 0; i < fixedWidthResultBlock.getPositionCount(); i++) {
            type.appendTo(fixedWidthResultBlock, i, variableWidthBlockBuilder);
        }

        return buildStructuralSlice(variableWidthBlockBuilder);
    }
}
