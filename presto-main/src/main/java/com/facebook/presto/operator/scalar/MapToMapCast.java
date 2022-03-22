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

import com.facebook.presto.annotation.UsedByGeneratedCode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.CastType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.JavaScalarFunctionImplementation;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueWriter;
import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.Signature.typeVariable;
import static com.facebook.presto.util.Failures.internalError;
import static com.facebook.presto.util.Reflection.methodHandle;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Primitives.unwrap;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodType.methodType;

public final class MapToMapCast
        extends SqlOperator
{
    public static final MapToMapCast MAP_TO_MAP_CAST = new MapToMapCast();

    private static final MethodHandle METHOD_HANDLE = methodHandle(
            MapToMapCast.class,
            "mapCast",
            MethodHandle.class,
            MethodHandle.class,
            Type.class,
            SqlFunctionProperties.class,
            Block.class);

    private static final MethodHandle CHECK_LONG_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkLongIsNotNull", Long.class);
    private static final MethodHandle CHECK_DOUBLE_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkDoubleIsNotNull", Double.class);
    private static final MethodHandle CHECK_BOOLEAN_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkBooleanIsNotNull", Boolean.class);
    private static final MethodHandle CHECK_SLICE_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkSliceIsNotNull", Slice.class);
    private static final MethodHandle CHECK_BLOCK_IS_NOT_NULL = methodHandle(MapToMapCast.class, "checkBlockIsNotNull", Block.class);

    public MapToMapCast()
    {
        super(CAST,
                ImmutableList.of(typeVariable("FK"), typeVariable("FV"), typeVariable("TK"), typeVariable("TV")),
                ImmutableList.of(),
                parseTypeSignature("map(TK,TV)"),
                ImmutableList.of(parseTypeSignature("map(FK,FV)")));
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type fromKeyType = boundVariables.getTypeVariable("FK");
        Type fromValueType = boundVariables.getTypeVariable("FV");
        Type toKeyType = boundVariables.getTypeVariable("TK");
        Type toValueType = boundVariables.getTypeVariable("TV");
        Type toMapType = functionAndTypeManager.getParameterizedType(
                "map",
                ImmutableList.of(
                        TypeSignatureParameter.of(toKeyType.getTypeSignature()),
                        TypeSignatureParameter.of(toValueType.getTypeSignature())));

        MethodHandle keyProcessor = buildProcessor(functionAndTypeManager, fromKeyType, toKeyType, true);
        MethodHandle valueProcessor = buildProcessor(functionAndTypeManager, fromValueType, toValueType, false);
        MethodHandle target = MethodHandles.insertArguments(METHOD_HANDLE, 0, keyProcessor, valueProcessor, toMapType);
        return new BuiltInScalarFunctionImplementation(true, ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)), target);
    }

    /**
     * The signature of the returned MethodHandle is (Block fromMap, int position, SqlFunctionProperties properties, BlockBuilder mapBlockBuilder)void.
     * The processor will get the value from fromMap, cast it and write to toBlock.
     */
    private MethodHandle buildProcessor(FunctionAndTypeManager functionAndTypeManager, Type fromType, Type toType, boolean isKey)
    {
        MethodHandle getter = nativeValueGetter(fromType);

        // Adapt cast that takes ([SqlFunctionProperties,] ?) to one that takes (?, SqlFunctionProperties), where ? is the return type of getter.
        JavaScalarFunctionImplementation castImplementation = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.lookupCast(CastType.CAST, fromType.getTypeSignature(), toType.getTypeSignature()));
        MethodHandle cast = castImplementation.getMethodHandle();
        if (cast.type().parameterArray()[0] != SqlFunctionProperties.class) {
            cast = MethodHandles.dropArguments(cast, 0, SqlFunctionProperties.class);
        }
        cast = permuteArguments(cast, methodType(cast.type().returnType(), cast.type().parameterArray()[1], cast.type().parameterArray()[0]), 1, 0);
        MethodHandle target = compose(cast, getter);

        // If the key cast function is nullable, check the result is not null.
        if (isKey && castImplementation.isNullable()) {
            target = compose(nullChecker(target.type().returnType()), target);
        }

        MethodHandle writer = nativeValueWriter(toType);
        writer = permuteArguments(writer, methodType(void.class, writer.type().parameterArray()[1], writer.type().parameterArray()[0]), 1, 0);

        return compose(writer, target.asType(methodType(unwrap(target.type().returnType()), target.type().parameterArray())));
    }

    /**
     * Returns a null checker MethodHandle that only returns the value when it is not null.
     * <p>
     * The signature of the returned MethodHandle could be one of the following depending on javaType:
     * <ul>
     * <li>(Long value)long
     * <li>(Double value)double
     * <li>(Boolean value)boolean
     * <li>(Slice value)Slice
     * <li>(Block value)Block
     * </ul>
     */
    private MethodHandle nullChecker(Class<?> javaType)
    {
        if (javaType == Long.class) {
            return CHECK_LONG_IS_NOT_NULL;
        }
        else if (javaType == Double.class) {
            return CHECK_DOUBLE_IS_NOT_NULL;
        }
        else if (javaType == Boolean.class) {
            return CHECK_BOOLEAN_IS_NOT_NULL;
        }
        else if (javaType == Slice.class) {
            return CHECK_SLICE_IS_NOT_NULL;
        }
        else if (javaType == Block.class) {
            return CHECK_BLOCK_IS_NOT_NULL;
        }
        else {
            throw new IllegalArgumentException("Unknown java type " + javaType);
        }
    }

    @UsedByGeneratedCode
    public static long checkLongIsNotNull(Long value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static double checkDoubleIsNotNull(Double value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static boolean checkBooleanIsNotNull(Boolean value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Slice checkSliceIsNotNull(Slice value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Block checkBlockIsNotNull(Block value)
    {
        if (value == null) {
            throw new PrestoException(INVALID_CAST_ARGUMENT, "map key is null");
        }
        return value;
    }

    @UsedByGeneratedCode
    public static Block mapCast(
            MethodHandle keyProcessFunction,
            MethodHandle valueProcessFunction,
            Type toMapType,
            SqlFunctionProperties properties,
            Block fromMap)
    {
        checkState(toMapType.getTypeParameters().size() == 2, "Expect two type parameters for toMapType");
        Type toKeyType = toMapType.getTypeParameters().get(0);
        TypedSet typedSet = new TypedSet(toKeyType, fromMap.getPositionCount() / 2, "map-to-map cast");
        BlockBuilder keyBlockBuilder = toKeyType.createBlockBuilder(null, fromMap.getPositionCount() / 2);
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            try {
                keyProcessFunction.invokeExact(fromMap, i, properties, keyBlockBuilder);
            }
            catch (Throwable t) {
                throw internalError(t);
            }
        }
        Block keyBlock = keyBlockBuilder.build();

        BlockBuilder mapBlockBuilder = toMapType.createBlockBuilder(null, 1);
        BlockBuilder blockBuilder = mapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            if (!typedSet.contains(keyBlock, i / 2)) {
                typedSet.add(keyBlock, i / 2);
                toKeyType.appendTo(keyBlock, i / 2, blockBuilder);
                if (fromMap.isNull(i + 1)) {
                    blockBuilder.appendNull();
                    continue;
                }

                try {
                    valueProcessFunction.invokeExact(fromMap, i + 1, properties, blockBuilder);
                }
                catch (Throwable t) {
                    throw internalError(t);
                }
            }
            else {
                // if there are duplicated keys, fail it!
                throw new PrestoException(INVALID_CAST_ARGUMENT, "duplicate keys");
            }
        }

        mapBlockBuilder.closeEntry();
        return (Block) toMapType.getObject(mapBlockBuilder, mapBlockBuilder.getPositionCount() - 1);
    }
}
