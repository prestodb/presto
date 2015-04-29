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
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.MapType;
import com.facebook.presto.type.RowType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.OperatorType.CAST;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;
import static com.facebook.presto.type.TypeUtils.readStructuralBlock;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.String.format;

public final class ArrayJoin
        extends ParametricScalar
{
    public static final ArrayJoin ARRAY_JOIN = new ArrayJoin();
    private static final TypeSignature VARCHAR_TYPE_SIGNATURE = parseTypeSignature(StandardTypes.VARCHAR);
    private static final String FUNCTION_NAME = "array_join";
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(typeParameter("T")), StandardTypes.VARCHAR,
            ImmutableList.of("array<T>", StandardTypes.VARCHAR, StandardTypes.VARCHAR),
            false, false);

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
        return "Concatenates the elements of the given array using a delimiter and an optional string to replace nulls";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        TypeSignature valueType = type.getTypeSignature();
        TypeSignature arrayType = parameterizedTypeName(StandardTypes.ARRAY, valueType);
        Signature signature = new Signature(FUNCTION_NAME, VARCHAR_TYPE_SIGNATURE, arrayType, VARCHAR_TYPE_SIGNATURE, VARCHAR_TYPE_SIGNATURE);

        MethodHandle methodHandle = methodHandle(ArrayJoin.class, "arrayJoin", FunctionInfo.class, Type.class, ConnectorSession.class, Slice.class, Slice.class, Slice.class);
        FunctionInfo castFunction = functionRegistry.getExactFunction(internalOperator(CAST.name(), VARCHAR_TYPE_SIGNATURE, ImmutableList.of(valueType)));

        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle.bindTo(castFunction).bindTo(type), isDeterministic(), false, ImmutableList.of(false, false, true));
    }

    public static Slice arrayJoin(FunctionInfo castFunction, Type elementType, ConnectorSession session, Slice array, Slice delimiter, Slice nullReplacement) throws Throwable
    {
        if (elementType instanceof ArrayType || elementType instanceof MapType || elementType instanceof RowType) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, format("Unexpected element type %s", elementType.getDisplayName()));
        }

        Block arrayBlock = readStructuralBlock(array);
        int numElements = arrayBlock.getPositionCount();
        DynamicSliceOutput sliceOutput = new DynamicSliceOutput(array.length());
        Class<?> javaType = elementType.getJavaType();

        for (int i = 0; i < numElements; i++) {
            if (arrayBlock.isNull(i)) {
                if (nullReplacement != null) {
                    sliceOutput.appendBytes(nullReplacement);
                }
            }
            else {
                if (javaType == boolean.class) {
                    sliceOutput.appendBytes((Slice) castFunction.getMethodHandle().invokeWithArguments(elementType.getBoolean(arrayBlock, i)));
                }
                else if (javaType == double.class) {
                    sliceOutput.appendBytes((Slice) castFunction.getMethodHandle().invokeWithArguments(elementType.getDouble(arrayBlock, i)));
                }
                else if (javaType == long.class) {
                    long value = elementType.getLong(arrayBlock, i);
                    Slice slice;
                    if (elementType == TIMESTAMP) {
                        slice = (Slice) castFunction.getMethodHandle().invokeWithArguments(session, value);
                    }
                    else {
                        slice = (Slice) castFunction.getMethodHandle().invokeWithArguments(value);
                    }
                    sliceOutput.appendBytes(slice);
                }
                else if (javaType == Slice.class) {
                    sliceOutput.appendBytes(elementType.getSlice(arrayBlock, i));
                }
                else if (javaType == void.class) {
                    //nop
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
}
