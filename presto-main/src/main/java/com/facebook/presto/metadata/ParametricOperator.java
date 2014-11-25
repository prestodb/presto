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
package com.facebook.presto.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ParametricOperator
        implements ParametricFunction
{
    private final OperatorType operatorType;
    private final List<TypeParameter> typeParameters;
    private final String returnType;
    private final List<String> argumentTypes;

    protected ParametricOperator(OperatorType operatorType, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes)
    {
        this.typeParameters = ImmutableList.copyOf(checkNotNull(typeParameters, "typeParameters is null"));
        this.returnType = checkNotNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(checkNotNull(argumentTypes, "argumentTypes is null"));
        this.operatorType = checkNotNull(operatorType, "operatorType is null");
    }

    @Override
    public Signature getSignature()
    {
        return new Signature(mangleOperatorName(operatorType), typeParameters, returnType, argumentTypes, false, true);
    }

    @Override
    public final boolean isScalar()
    {
        return true;
    }

    @Override
    public final boolean isAggregate()
    {
        return false;
    }

    @Override
    public final boolean isHidden()
    {
        return true;
    }

    @Override
    public final boolean isApproximate()
    {
        return false;
    }

    @Override
    public final boolean isWindow()
    {
        return false;
    }

    @Override
    public final boolean isDeterministic()
    {
        return true;
    }

    @Override
    public final boolean isUnbound()
    {
        return true;
    }

    @Override
    public final String getDescription()
    {
        // Operators are internal, and don't need a description
        return null;
    }
}
