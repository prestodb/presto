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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Stream.concat;

public final class Signature
{
    private final String name;
    private final FunctionKind kind;
    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final List<LongVariableConstraint> longVariableConstraints;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    @JsonCreator
    public Signature(
            @JsonProperty("name") String name,
            @JsonProperty("kind") FunctionKind kind,
            @JsonProperty("typeVariableConstraints") List<TypeVariableConstraint> typeVariableConstraints,
            @JsonProperty("longVariableConstraints") List<LongVariableConstraint> longVariableConstraints,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeVariableConstraints, "typeVariableConstraints is null");
        requireNonNull(longVariableConstraints, "longVariableConstraints is null");

        this.name = name;
        this.kind = requireNonNull(kind, "type is null");
        this.typeVariableConstraints = ImmutableList.copyOf(typeVariableConstraints);
        this.longVariableConstraints = ImmutableList.copyOf(longVariableConstraints);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.variableArity = variableArity;
    }

    public Signature(String name, FunctionKind kind, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        this(name, kind, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public Signature(String name, FunctionKind kind, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        this(name, kind, ImmutableList.of(), ImmutableList.of(), returnType, argumentTypes, false);
    }

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(toImmutableList()));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalOperator(operator, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(OperatorType operator, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.of(), ImmutableList.of(), returnType, argumentTypes, false);
    }

    public Signature withAlias(String name)
    {
        return new Signature(name, kind, typeVariableConstraints, longVariableConstraints, getReturnType(), getArgumentTypes(), variableArity);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public FunctionKind getKind()
    {
        return kind;
    }

    @JsonProperty
    public TypeSignature getReturnType()
    {
        return returnType;
    }

    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    @JsonProperty
    public List<TypeVariableConstraint> getTypeVariableConstraints()
    {
        return typeVariableConstraints;
    }

    @JsonProperty
    public List<LongVariableConstraint> getLongVariableConstraints()
    {
        return longVariableConstraints;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, kind, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
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
        Signature other = (Signature) obj;
        return Objects.equals(this.name, other.name) &&
                Objects.equals(this.kind, other.kind) &&
                Objects.equals(this.typeVariableConstraints, other.typeVariableConstraints) &&
                Objects.equals(this.longVariableConstraints, other.longVariableConstraints) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity);
    }

    @Override
    public String toString()
    {
        List<String> allConstraints = concat(
                typeVariableConstraints.stream().map(TypeVariableConstraint::toString),
                longVariableConstraints.stream().map(LongVariableConstraint::toString))
                .collect(Collectors.toList());

        return name + (allConstraints.isEmpty() ? "" : "<" + Joiner.on(",").join(allConstraints) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    /*
     * similar to T extends MyClass<?...>, if Java supported varargs wildcards
     */
    public static TypeVariableConstraint withVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, false, false, variadicBound);
    }

    public static TypeVariableConstraint comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, true, false, variadicBound);
    }

    public static TypeVariableConstraint typeVariable(String name)
    {
        return new TypeVariableConstraint(name, false, false, null);
    }

    public static TypeVariableConstraint comparableTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, true, false, null);
    }

    public static TypeVariableConstraint orderableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, false, true, variadicBound);
    }

    public static TypeVariableConstraint orderableTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, false, true, null);
    }

    public static LongVariableConstraint longVariableExpression(String variable, String expression)
    {
        return new LongVariableConstraint(variable, expression);
    }

    public static SignatureBuilder builder()
    {
        return new SignatureBuilder();
    }
}
