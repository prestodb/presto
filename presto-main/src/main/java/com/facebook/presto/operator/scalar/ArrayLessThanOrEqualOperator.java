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
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.metadata.Signature.orderableTypeParameter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;

public class ArrayLessThanOrEqualOperator
        extends ParametricOperator
{
    public static final ArrayLessThanOrEqualOperator ARRAY_LESS_THAN_OR_EQUAL = new ArrayLessThanOrEqualOperator();
    private static final TypeSignature RETURN_TYPE = parseTypeSignature(StandardTypes.BOOLEAN);
    public static final MethodHandle METHOD_HANDLE = methodHandle(ArrayLessThanOrEqualOperator.class, "lessThanOrEqual", Type.class, Slice.class, Slice.class);

    private ArrayLessThanOrEqualOperator()
    {
        super(LESS_THAN_OR_EQUAL, ImmutableList.of(orderableTypeParameter("T")), StandardTypes.BOOLEAN, ImmutableList.of("array<T>", "array<T>"));
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        Type type = types.get("T");
        type = typeManager.getParameterizedType(StandardTypes.ARRAY, ImmutableList.of(type.getTypeSignature()), ImmutableList.of());
        TypeSignature typeSignature = type.getTypeSignature();
        return operatorInfo(LESS_THAN_OR_EQUAL, RETURN_TYPE, ImmutableList.of(typeSignature, typeSignature), METHOD_HANDLE.bindTo(type), false, ImmutableList.of(false, false));
    }

    public static boolean lessThanOrEqual(Type type, Slice left, Slice right)
    {
        BlockBuilder leftBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        BlockBuilder rightBlockBuilder = type.createBlockBuilder(new BlockBuilderStatus());
        leftBlockBuilder.writeBytes(left, 0, left.length());
        rightBlockBuilder.writeBytes(right, 0, right.length());
        return type.compareTo(leftBlockBuilder.closeEntry().build(), 0, rightBlockBuilder.closeEntry().build(), 0) <= 0;
    }
}
