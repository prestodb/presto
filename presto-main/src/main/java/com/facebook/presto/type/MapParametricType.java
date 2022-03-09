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

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.ParameterKind;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.metadata.FunctionAndTypeManager;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.facebook.presto.common.block.MethodHandleUtil.compose;
import static com.facebook.presto.common.block.MethodHandleUtil.nativeValueGetter;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.google.common.base.Preconditions.checkArgument;

public final class MapParametricType
        implements ParametricType
{
    public static final MapParametricType MAP = new MapParametricType();

    @Override
    public String getName()
    {
        return StandardTypes.MAP;
    }

    @Override
    public Type createType(List<TypeParameter> parameters)
    {
        throw new IllegalStateException("MapType creation requires map operator MethodHandle. Please use `createType(functionAndTypeManager, parameters)` instead");
    }

    public Type createType(FunctionAndTypeManager functionAndTypeManager, List<TypeParameter> parameters)
    {
        checkArgument(parameters.size() == 2, "Expected two parameters, got %s", parameters);
        TypeParameter firstParameter = parameters.get(0);
        TypeParameter secondParameter = parameters.get(1);
        checkArgument(
                firstParameter.getKind() == ParameterKind.TYPE && secondParameter.getKind() == ParameterKind.TYPE,
                "Expected key and type to be types, got %s",
                parameters);

        Type keyType = firstParameter.getType();
        Type valueType = secondParameter.getType();
        MethodHandle keyNativeEquals = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.EQUAL, fromTypes(keyType, keyType))).getMethodHandle();
        MethodHandle keyBlockEquals = compose(keyNativeEquals, nativeValueGetter(keyType), nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = functionAndTypeManager.getJavaScalarFunctionImplementation(functionAndTypeManager.resolveOperator(OperatorType.HASH_CODE, fromTypes(keyType))).getMethodHandle();
        MethodHandle keyBlockHashCode = compose(keyNativeHashCode, nativeValueGetter(keyType));
        return new MapType(
                keyType,
                valueType,
                keyBlockEquals,
                keyBlockHashCode);
    }
}
