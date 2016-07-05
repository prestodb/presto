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
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionKind;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class ArrayJoin
        extends SqlScalarFunction
{
    public static final ArrayJoin ARRAY_JOIN = new ArrayJoin();
    public static final ArrayJoinWithNullReplacement ARRAY_JOIN_WITH_NULL_REPLACEMENT = new ArrayJoinWithNullReplacement();

    private static final TypeSignature VARCHAR_TYPE_SIGNATURE = VARCHAR.getTypeSignature();
    private static final String FUNCTION_NAME = "array_join";
    private static final String DESCRIPTION = "Concatenates the elements of the given array using a delimiter and an optional string to replace nulls";
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayJoin.class, "arrayJoin", MethodHandle.class, Type.class, ConnectorSession.class, Block.class, Slice.class);

    public static class ArrayJoinWithNullReplacement
            extends SqlScalarFunction
    {
        private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayJoin.class, "arrayJoin", MethodHandle.class, Type.class, ConnectorSession.class, Block.class, Slice.class, Slice.class);

        public ArrayJoinWithNullReplacement()
        {
            super(new Signature(FUNCTION_NAME,
                    FunctionKind.SCALAR,
                    ImmutableList.of(typeVariable("T")),
                    ImmutableList.of(),
                    parseTypeSignature(StandardTypes.VARCHAR),
                    ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)),
                    false));
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
            return DESCRIPTION;
        }

        @Override
        public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Type type = boundVariables.getTypeVariable("T");
            TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, type.getTypeSignature());
            Signature signature = new Signature(FUNCTION_NAME, SCALAR, VARCHAR_TYPE_SIGNATURE, arrayType, VARCHAR_TYPE_SIGNATURE, VARCHAR_TYPE_SIGNATURE);
            return specializeArrayJoin(boundVariables.getTypeVariables(), functionRegistry, ImmutableList.of(false, false, false), signature, METHOD_HANDLE);
        }
    }

    public ArrayJoin()
    {
        super(new Signature(FUNCTION_NAME,
                FunctionKind.SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature(StandardTypes.VARCHAR),
                ImmutableList.of(parseTypeSignature("array(T)"), parseTypeSignature(StandardTypes.VARCHAR)),
                false));
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
        return DESCRIPTION;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = boundVariables.getTypeVariable("T");
        TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, type.getTypeSignature());
        Signature signature = new Signature(FUNCTION_NAME, SCALAR, VARCHAR_TYPE_SIGNATURE, arrayType, VARCHAR_TYPE_SIGNATURE);
        return specializeArrayJoin(boundVariables.getTypeVariables(), functionRegistry, ImmutableList.of(false, false), signature, METHOD_HANDLE);
    }

    private static ScalarFunctionImplementation specializeArrayJoin(Map<String, Type> types, FunctionRegistry functionRegistry, List<Boolean> nullableArguments, Signature signature, MethodHandle methodHandle)
    {
        Type type = types.get("T");
        if (type instanceof UnknownType) {
            return new ScalarFunctionImplementation(false, nullableArguments, methodHandle.bindTo(null).bindTo(type), true);
        }
        else {
            try {
                ScalarFunctionImplementation castFunction = functionRegistry.getScalarFunctionImplementation(internalOperator(CAST.name(), VARCHAR_TYPE_SIGNATURE, ImmutableList.of(type.getTypeSignature())));
                return new ScalarFunctionImplementation(false, nullableArguments, methodHandle.bindTo(castFunction.getMethodHandle()).bindTo(type), true);
            }
            catch (PrestoException e) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Input type %s not supported", type), e);
            }
        }
    }

    @UsedByGeneratedCode
    public static Slice arrayJoin(MethodHandle castFunction, Type elementType, ConnectorSession session, Block arrayBlock, Slice delimiter)
    {
        return arrayJoin(castFunction, elementType, session, arrayBlock, delimiter, null);
    }

    public static Slice arrayJoin(MethodHandle castFunction, Type elementType, ConnectorSession session, Block arrayBlock, Slice delimiter, Slice nullReplacement)
    {
        int numElements = arrayBlock.getPositionCount();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(arrayBlock.getSizeInBytes() + delimiter.length() * arrayBlock.getPositionCount());
        Class<?> javaType = elementType.getJavaType();

        Class<?>[] parameters = null;
        // can be null for the unknown type
        if (castFunction != null) {
            parameters = castFunction.type().parameterArray();
        }

        for (int i = 0; i < numElements; i++) {
            if (arrayBlock.isNull(i)) {
                if (nullReplacement != null) {
                    sliceOutput.appendBytes(nullReplacement);
                }
                else {
                    continue;
                }
            }
            else {
                if (javaType == boolean.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunction, session, elementType.getBoolean(arrayBlock, i)));
                }
                else if (javaType == double.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunction, session, elementType.getDouble(arrayBlock, i)));
                }
                else if (javaType == long.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunction, session, elementType.getLong(arrayBlock, i)));
                }
                else if (javaType == Slice.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunction, session, elementType.getSlice(arrayBlock, i)));
                }
                else {
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected type %s", javaType.getName()));
                }
            }

            if (i != numElements - 1) {
                sliceOutput.appendBytes(delimiter);
            }
        }

        return sliceOutput.slice();
    }

    private static Slice invokeCast(Class<?>[] castFunctionParameters, MethodHandle castFunctionHandle, ConnectorSession session, Object arg)
    {
        Slice slice;
        try {
            if (castFunctionParameters[0] == ConnectorSession.class) {
                slice = (Slice) castFunctionHandle.invokeWithArguments(session, arg);
            }
            else {
                slice = (Slice) castFunctionHandle.invokeWithArguments(arg);
            }
        }
        catch (Throwable throwable) {
            throw new PrestoException(GENERIC_INTERNAL_ERROR, format("Error casting array element %s to VARCHAR", arg));
        }
        return slice;
    }
}
