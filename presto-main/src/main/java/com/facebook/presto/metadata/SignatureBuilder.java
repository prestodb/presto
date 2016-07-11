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

import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.TypeSignature;

import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.google.common.collect.ImmutableList.copyOf;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public final class SignatureBuilder
{
    private String name;
    private FunctionKind kind;
    private List<TypeVariableConstraint> typeVariableConstraints = emptyList();
    private List<LongVariableConstraint> longVariableConstraints = emptyList();
    private TypeSignature returnType;
    private List<TypeSignature> argumentTypes = emptyList();
    private boolean variableArity;

    public SignatureBuilder() {}

    public SignatureBuilder name(String name)
    {
        this.name = requireNonNull(name, "name is null");
        return this;
    }

    public SignatureBuilder kind(FunctionKind kind)
    {
        this.kind = kind;
        return this;
    }

    public SignatureBuilder operatorType(OperatorType operatorType)
    {
        this.name = mangleOperatorName(requireNonNull(operatorType, "operatorType is null"));
        this.kind = SCALAR;
        return this;
    }

    public SignatureBuilder typeVariableConstraints(TypeVariableConstraint... typeVariableConstraints)
    {
        return typeVariableConstraints(asList(requireNonNull(typeVariableConstraints, "typeVariableConstraints is null")));
    }

    public SignatureBuilder typeVariableConstraints(List<TypeVariableConstraint> typeVariableConstraints)
    {
        this.typeVariableConstraints = copyOf(requireNonNull(typeVariableConstraints, "typeVariableConstraints is null"));
        return this;
    }

    public SignatureBuilder returnType(TypeSignature returnType)
    {
        this.returnType = requireNonNull(returnType, "returnType is null");
        return this;
    }

    public SignatureBuilder longVariableConstraints(LongVariableConstraint... longVariableConstraints)
    {
        return longVariableConstraints(asList(requireNonNull(longVariableConstraints, "longVariableConstraints is null")));
    }

    public SignatureBuilder longVariableConstraints(List<LongVariableConstraint> longVariableConstraints)
    {
        this.longVariableConstraints = copyOf(requireNonNull(longVariableConstraints, "longVariableConstraints is null"));
        return this;
    }

    public SignatureBuilder argumentTypes(TypeSignature... argumentTypes)
    {
        return argumentTypes(asList(requireNonNull(argumentTypes, "argumentTypes is Null")));
    }

    public SignatureBuilder argumentTypes(List<TypeSignature> argumentTypes)
    {
        this.argumentTypes = copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        return this;
    }

    public SignatureBuilder setVariableArity(boolean variableArity)
    {
        this.variableArity = variableArity;
        return this;
    }

    public Signature build()
    {
        return new Signature(name, kind, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
    }
}
