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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.prestosql.annotation.UsedByGeneratedCode;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.metadata.SqlOperator;
import io.prestosql.operator.aggregation.TypedSet;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSignatureParameter;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.primitives.Primitives.unwrap;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static io.prestosql.operator.scalar.ScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static io.prestosql.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueWriter;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.util.Failures.internalError;
import static io.prestosql.util.Reflection.methodHandle;
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
            ConnectorSession.class,
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
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(arity == 1, "Expected arity to be 1");
        Type fromKeyType = boundVariables.getTypeVariable("FK");
        Type fromValueType = boundVariables.getTypeVariable("FV");
        Type toKeyType = boundVariables.getTypeVariable("TK");
        Type toValueType = boundVariables.getTypeVariable("TV");
        Type toMapType = typeManager.getParameterizedType(
                "map",
                ImmutableList.of(
                        TypeSignatureParameter.of(toKeyType.getTypeSignature()),
                        TypeSignatureParameter.of(toValueType.getTypeSignature())));

        MethodHandle keyProcessor = buildProcessor(functionRegistry, fromKeyType, toKeyType, true);
        MethodHandle valueProcessor = buildProcessor(functionRegistry, fromValueType, toValueType, false);
        MethodHandle target = MethodHandles.insertArguments(METHOD_HANDLE, 0, keyProcessor, valueProcessor, toMapType);
        return new ScalarFunctionImplementation(true, ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)), target, true);
    }

    /**
     * The signature of the returned MethodHandle is (Block fromMap, int position, ConnectorSession session, BlockBuilder mapBlockBuilder)void.
     * The processor will get the value from fromMap, cast it and write to toBlock.
     */
    private MethodHandle buildProcessor(FunctionRegistry functionRegistry, Type fromType, Type toType, boolean isKey)
    {
        MethodHandle getter = nativeValueGetter(fromType);

        // Adapt cast that takes ([ConnectorSession,] ?) to one that takes (?, ConnectorSession), where ? is the return type of getter.
        ScalarFunctionImplementation castImplementation = functionRegistry.getScalarFunctionImplementation(functionRegistry.getCoercion(fromType, toType));
        MethodHandle cast = castImplementation.getMethodHandle();
        if (cast.type().parameterArray()[0] != ConnectorSession.class) {
            cast = MethodHandles.dropArguments(cast, 0, ConnectorSession.class);
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
            ConnectorSession session,
            Block fromMap)
    {
        checkState(toMapType.getTypeParameters().size() == 2, "Expect two type parameters for toMapType");
        Type toKeyType = toMapType.getTypeParameters().get(0);
        TypedSet typedSet = new TypedSet(toKeyType, fromMap.getPositionCount() / 2, "map-to-map cast");
        BlockBuilder keyBlockBuilder = toKeyType.createBlockBuilder(null, fromMap.getPositionCount() / 2);
        for (int i = 0; i < fromMap.getPositionCount(); i += 2) {
            try {
                keyProcessFunction.invokeExact(fromMap, i, session, keyBlockBuilder);
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
                    valueProcessFunction.invokeExact(fromMap, i + 1, session, blockBuilder);
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
