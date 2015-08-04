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

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.util.Failures.checkCondition;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class ArrayJoin
        extends ParametricScalar
{
    public static final ArrayJoin ARRAY_JOIN = new ArrayJoin();
    public static final ArrayJoinWithNullReplacement ARRAY_JOIN_WITH_NULL_REPLACEMENT = new ArrayJoinWithNullReplacement();

    private static final TypeSignature VARCHAR_TYPE_SIGNATURE = VARCHAR.getTypeSignature();
    private static final String FUNCTION_NAME = "array_join";
    private static final String DESCRIPTION = "Concatenates the elements of the given array using a delimiter and an optional string to replace nulls";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME,  ImmutableList.of(typeParameter("T")), StandardTypes.VARCHAR, ImmutableList.of("array<T>", StandardTypes.VARCHAR), false, false);
    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayJoin.class, "arrayJoin", FunctionInfo.class, Type.class, ConnectorSession.class, Block.class, Slice.class);

    public static class ArrayJoinWithNullReplacement
            extends ParametricScalar
    {
        private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("T")), StandardTypes.VARCHAR, ImmutableList.of("array<T>", StandardTypes.VARCHAR, StandardTypes.VARCHAR), false, false);
        private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayJoin.class, "arrayJoin", FunctionInfo.class, Type.class, ConnectorSession.class, Block.class, Slice.class, Slice.class);

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
            return DESCRIPTION;
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            Type type = types.get("T");
            TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, type.getTypeSignature());
            Signature signature = new Signature(FUNCTION_NAME, VARCHAR_TYPE_SIGNATURE, arrayType, VARCHAR_TYPE_SIGNATURE, VARCHAR_TYPE_SIGNATURE);
            return specializeArrayJoin(types, functionRegistry, ImmutableList.of(false, false, false), signature, METHOD_HANDLE);
        }
    }

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
        return DESCRIPTION;
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, type.getTypeSignature());
        Signature signature = new Signature(FUNCTION_NAME, VARCHAR_TYPE_SIGNATURE, arrayType, VARCHAR_TYPE_SIGNATURE);
        return specializeArrayJoin(types, functionRegistry, ImmutableList.of(false, false), signature, METHOD_HANDLE);
    }

    private static FunctionInfo specializeArrayJoin(Map<String, Type> types, FunctionRegistry functionRegistry, List<Boolean> nullableArguments, Signature signature, MethodHandle methodHandle)
    {
        Type type = types.get("T");
        FunctionInfo castFunction = functionRegistry.getExactFunction(internalOperator(CAST.name(), VARCHAR_TYPE_SIGNATURE, ImmutableList.of(type.getTypeSignature())));

        if (!(type instanceof UnknownType)) {
            checkCondition(castFunction != null, INVALID_FUNCTION_ARGUMENT, "Input type %s not supported", type);
        }

        return new FunctionInfo(signature, DESCRIPTION, false, methodHandle.bindTo(castFunction).bindTo(type), true, false, nullableArguments);
    }

    public static Slice arrayJoin(FunctionInfo castFunction, Type elementType, ConnectorSession session, Block arrayBlock, Slice delimiter)
    {
        return arrayJoin(castFunction, elementType, session, arrayBlock, delimiter, null);
    }

    public static Slice arrayJoin(FunctionInfo castFunction, Type elementType, ConnectorSession session, Block arrayBlock, Slice delimiter, Slice nullReplacement)
    {
        int numElements = arrayBlock.getPositionCount();

        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(arrayBlock.getSizeInBytes() + delimiter.length() * arrayBlock.getPositionCount());
        Class<?> javaType = elementType.getJavaType();

        Class<?>[] parameters = null;
        MethodHandle castFunctionHandle = null;
        // can be null for the unknown type
        if (castFunction != null) {
            parameters = castFunction.getMethodHandle().type().parameterArray();
            castFunctionHandle = castFunction.getMethodHandle();
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
                    sliceOutput.appendBytes(invokeCast(parameters, castFunctionHandle, session, elementType.getBoolean(arrayBlock, i)));
                }
                else if (javaType == double.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunctionHandle, session, elementType.getDouble(arrayBlock, i)));
                }
                else if (javaType == long.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunctionHandle, session, elementType.getLong(arrayBlock, i)));
                }
                else if (javaType == Slice.class) {
                    sliceOutput.appendBytes(invokeCast(parameters, castFunctionHandle, session, elementType.getSlice(arrayBlock, i)));
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
            throw new PrestoException(INTERNAL_ERROR, format("Error casting array element %s to VARCHAR", arg));
        }
        return slice;
    }
}
