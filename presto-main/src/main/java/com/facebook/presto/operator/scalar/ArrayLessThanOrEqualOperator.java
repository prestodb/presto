package com.facebook.presto.operator.scalar;
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

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.EQUAL;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayLessThanOrEqualOperator
        extends ParametricOperator
{
    public static final ArrayLessThanOrEqualOperator ARRAY_LESS_THAN_OR_EQUAL = new ArrayLessThanOrEqualOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);

    private ArrayLessThanOrEqualOperator()
    {
        super(LESS_THAN_OR_EQUAL, ImmutableList.of(orderableTypeParameter("T")), StandardTypes.BOOLEAN, ImmutableList.of("array<T>", "array<T>"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type elementType = types.get("T");
        Type type = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(elementType.getTypeSignature()), ImmutableList.of());
        TypeSignature typeSignature = type.getTypeSignature();
        MethodHandle lessThanFunction = functionRegistry.resolveOperator(LESS_THAN, ImmutableList.of(elementType, elementType)).getMethodHandle();
        MethodHandle equalsFunction = functionRegistry.resolveOperator(EQUAL, ImmutableList.of(elementType, elementType)).getMethodHandle();
        MethodHandle methodHandle = methodHandle(ArrayLessThanOrEqualOperator.class, "lessThanOrEqual", MethodHandle.class, MethodHandle.class, Type.class, Slice.class, Slice.class);
        MethodHandle method = methodHandle.bindTo(lessThanFunction).bindTo(equalsFunction).bindTo(elementType);
        return operatorInfo(LESS_THAN_OR_EQUAL, RETURN_TYPE, ImmutableList.of(typeSignature, typeSignature), method, false, ImmutableList.of(false, false));
    }

    public static boolean lessThanOrEqual(MethodHandle lessThanFunction, MethodHandle equalsFunction, Type type, Slice left, Slice right)
    {
        return ArrayLessThanOperator.lessThan(lessThanFunction, type, left, right) ||
                ArrayEqualOperator.equals(equalsFunction, type, left, right);
    }
}
