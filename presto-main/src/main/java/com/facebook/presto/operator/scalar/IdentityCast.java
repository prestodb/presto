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

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.google.common.base.Preconditions.checkArgument;

public class IdentityCast
        extends SqlOperator
{
    public static final IdentityCast IDENTITY_CAST = new IdentityCast();

    protected IdentityCast()
    {
        super(OperatorType.CAST, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("T"));
    }

    @Override
    public ScalarFunctionImplementation specialize(Map<String, Type> types, List<TypeSignature> parameterTypes, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        checkArgument(types.size() == 1, "Expected only one type");
        Type type = types.get("T");
        MethodHandle identity = MethodHandles.identity(type.getJavaType());
        return new ScalarFunctionImplementation(false, ImmutableList.of(false), identity, isDeterministic());
    }
}
