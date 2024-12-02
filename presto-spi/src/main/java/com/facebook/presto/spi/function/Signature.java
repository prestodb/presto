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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Stream.concat;

@ThriftStruct
public final class Signature
{
    private final QualifiedObjectName name;
    private final FunctionKind kind;
    private final List<TypeVariableConstraint> typeVariableConstraints;
    private final List<LongVariableConstraint> longVariableConstraints;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    @ThriftConstructor
    @JsonCreator
    public Signature(
            @JsonProperty("name") QualifiedObjectName name,
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
        this.typeVariableConstraints = unmodifiableList(new ArrayList<>(typeVariableConstraints));
        this.longVariableConstraints = unmodifiableList(new ArrayList<>(longVariableConstraints));
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = unmodifiableList(new ArrayList<>(requireNonNull(argumentTypes, "argumentTypes is null")));
        this.variableArity = variableArity;
    }

    public Signature(QualifiedObjectName name, FunctionKind kind, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        this(name, kind, returnType, unmodifiableList(Arrays.asList(argumentTypes)));
    }

    public Signature(QualifiedObjectName name, FunctionKind kind, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        this(name, kind, emptyList(), emptyList(), returnType, argumentTypes, false);
    }

    @ThriftField(1)
    @JsonProperty
    public QualifiedObjectName getName()
    {
        return name;
    }

    public String getNameSuffix()
    {
        return name.getObjectName();
    }

    @ThriftField(2)
    @JsonProperty
    public FunctionKind getKind()
    {
        return kind;
    }

    @ThriftField(3)
    @JsonProperty
    public TypeSignature getReturnType()
    {
        return returnType;
    }

    @ThriftField(4)
    @JsonProperty
    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @ThriftField(5)
    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    @ThriftField(6)
    @JsonProperty
    public List<TypeVariableConstraint> getTypeVariableConstraints()
    {
        return typeVariableConstraints;
    }

    @ThriftField(7)
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
                .collect(toList());

        return name + (allConstraints.isEmpty() ? "" : "<" + String.join(",", allConstraints) + ">") +
                "(" + String.join(",", argumentTypes.stream().map(TypeSignature::toString).collect(toList())) + "):" + returnType;
    }

    /*
     * Canonical (normalized i.e. erased type size bounds) form of signature instance.
     */
    public Signature canonicalization()
    {
        return new Signature(this.name, this.kind, new TypeSignature(this.returnType.getBase(), emptyList()),
                argumentTypes
                        .stream()
                        .map(argumentTypeSignature -> new TypeSignature(argumentTypeSignature.getBase(), emptyList())).collect(toList()));
    }

    /*
     * similar to T extends MyClass<?...>, if Java supported varargs wildcards
     */
    public static TypeVariableConstraint withVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, false, false, variadicBound, false);
    }

    public static TypeVariableConstraint comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, true, false, variadicBound, false);
    }

    public static TypeVariableConstraint typeVariable(String name)
    {
        return new TypeVariableConstraint(name, false, false, null, false);
    }

    public static TypeVariableConstraint comparableTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, true, false, null, false);
    }

    public static TypeVariableConstraint orderableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeVariableConstraint(name, false, true, variadicBound, false);
    }

    public static TypeVariableConstraint orderableTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, false, true, null, false);
    }

    public static TypeVariableConstraint nonDecimalNumericTypeParameter(String name)
    {
        return new TypeVariableConstraint(name, false, false, null, true);
    }

    public static LongVariableConstraint longVariableExpression(String variable, String expression)
    {
        return new LongVariableConstraint(variable, expression);
    }
}
