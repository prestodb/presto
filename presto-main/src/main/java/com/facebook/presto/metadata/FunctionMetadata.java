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

import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    private final String name;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final FunctionKind functionKind;
    private final Optional<OperatorType> operatorType;
    private final boolean deterministic;
    private final boolean calledOnNullInput;

    public FunctionMetadata(
            String name,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionKind functionKind,
            Optional<OperatorType> operatorType,
            boolean deterministic,
            boolean calledOnNullInput)
    {
        this.name = requireNonNull(name, "name is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.functionKind = requireNonNull(functionKind, "functionKind is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.deterministic = deterministic;
        this.calledOnNullInput = calledOnNullInput;
    }

    public FunctionKind getFunctionKind()
    {
        return functionKind;
    }

    public String getName()
    {
        return name;
    }

    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    public TypeSignature getReturnType()
    {
        return returnType;
    }

    public Optional<OperatorType> getOperatorType()
    {
        return operatorType;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public boolean isCalledOnNullInput()
    {
        return calledOnNullInput;
    }
}
