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
import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.Signature.typeParameter;
import static com.google.common.base.Preconditions.checkArgument;

public class IdentityCastParametricFunction
        extends ParametricOperator
{
    public static final IdentityCastParametricFunction IDENTITY_CAST = new IdentityCastParametricFunction();

    protected IdentityCastParametricFunction()
    {
        super(OperatorType.CAST, ImmutableList.of(typeParameter("T")), "T", ImmutableList.of("T"));
    }

    @Override
    public String getDescription()
    {
        // Internal function, so it doesn't need one
        return "";
    }

    @Override
    public FunctionInfo specialize(List<? extends Type> types)
    {
        checkArgument(types.size() == 1, "Expected only one type");
        Type type = types.get(0);
        MethodHandle identity = MethodHandles.identity(type.getJavaType());
        return operatorInfo(OperatorType.CAST, type.getName(), ImmutableList.of(type.getName()), identity, false, ImmutableList.of(false));
    }

    @Override
    public FunctionInfo specialize(Type returnType, List<? extends Type> types)
    {
        return specialize(types);
    }
}
