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
package com.facebook.presto.spi.function;

import com.facebook.presto.spi.type.TypeSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    private final String name;
    private final Optional<OperatorType> operatorType;
    private final List<TypeSignature> argumentTypes;
    private final TypeSignature returnType;
    private final FunctionKind functionKind;
    private final boolean deterministic;
    private final boolean calledOnNullInput;

    public FunctionMetadata(
            String name,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionKind functionKind,
            boolean deterministic,
            boolean calledOnNullInput)
    {
        this(name, Optional.empty(), argumentTypes, returnType, functionKind, deterministic, calledOnNullInput);
    }

    public FunctionMetadata(
            OperatorType operatorType,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionKind functionKind,
            boolean deterministic,
            boolean calledOnNullInput)
    {
        this(operatorType.getFunctionName(), Optional.of(operatorType), argumentTypes, returnType, functionKind, deterministic, calledOnNullInput);
    }

    private FunctionMetadata(
            String name,
            Optional<OperatorType> operatorType,
            List<TypeSignature> argumentTypes,
            TypeSignature returnType,
            FunctionKind functionKind,
            boolean deterministic,
            boolean calledOnNullInput)
    {
        this.name = requireNonNull(name, "name is null");
        this.operatorType = requireNonNull(operatorType, "operatorType is null");
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.functionKind = requireNonNull(functionKind, "functionKind is null");
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

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FunctionMetadata other = (FunctionMetadata) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.operatorType, other.operatorType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.functionKind, other.functionKind) &&
                Objects.equals(this.deterministic, other.deterministic) &&
                Objects.equals(this.calledOnNullInput, other.calledOnNullInput);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, operatorType, argumentTypes, returnType, functionKind, deterministic, calledOnNullInput);
    }
}
