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

import com.facebook.presto.type.TypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.facebook.presto.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.FunctionRegistry.canCoerce;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class Signature
{
    private final String name;
    private final List<TypeParameter> typeParameters;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;
    private final boolean internal;

    @JsonCreator
    public Signature(
            @JsonProperty("name") String name,
            @JsonProperty("typeParameters") List<TypeParameter> typeParameters,
            @JsonProperty("returnType") String returnType,
            @JsonProperty("argumentTypes") List<String> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity,
            @JsonProperty("internal") boolean internal)
    {
        checkNotNull(name, "name is null");
        checkNotNull(typeParameters, "typeParameters is null");
        checkNotNull(returnType, "returnType is null");
        checkNotNull(argumentTypes, "argumentTypes is null");

        this.name = name;
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.returnType = parseTypeSignature(checkNotNull(returnType, "returnType is null"));
        this.argumentTypes = FluentIterable.from(argumentTypes).transform(new Function<String, TypeSignature>()
        {
            @Override
            public TypeSignature apply(String input)
            {
                checkNotNull(input, "input is null");
                return parseTypeSignature(input);
            }
        }).toList();
        this.variableArity = variableArity;
        this.internal = internal;
    }

    public Signature(String name, String returnType, List<String> argumentTypes)
    {
        this(name, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false, false);
    }

    public Signature(String name, String returnType, String... argumentTypes)
    {
        this(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(String name, String returnType, List<String> argumentTypes)
    {
        return internalFunction(mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, String returnType, String... argumentTypes)
    {
        return internalFunction(mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalFunction(String name, String returnType, String... argumentTypes)
    {
        return internalFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalFunction(String name, String returnType, List<String> argumentTypes)
    {
        return new Signature(name, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false, true);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public String getReturnType()
    {
        return returnType.toString();
    }

    @JsonProperty
    public List<String> getArgumentTypes()
    {
        return FluentIterable.from(argumentTypes).transform(new Function<TypeSignature, String>() {
            @Override
            public String apply(TypeSignature input)
            {
                return input.toString();
            }
        }).toList();
    }

    @JsonProperty
    public boolean isInternal()
    {
        return internal;
    }

    @JsonProperty
    public boolean isVariableArity()
    {
        return variableArity;
    }

    @JsonProperty
    public List<TypeParameter> getTypeParameters()
    {
        return typeParameters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, typeParameters, returnType, argumentTypes, variableArity, internal);
    }

    Signature withAlias(String name)
    {
        return new Signature(name, typeParameters, getReturnType(), getArgumentTypes(), variableArity, internal);
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
                Objects.equals(this.typeParameters, other.typeParameters) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity) &&
                Objects.equals(this.internal, other.internal);
    }

    @Override
    public String toString()
    {
        return (internal ? "%" : "") + name + (typeParameters.isEmpty() ? "" : "<" + Joiner.on(",").join(typeParameters) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    public boolean match(Type returnType, List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        Map<String, TypeParameter> unboundParameters = new HashMap<>();
        for (TypeParameter parameter : typeParameters) {
            unboundParameters.put(parameter.getName(), parameter);
        }

        if (!matchAndBind(boundParameters, unboundParameters, this.returnType, returnType, allowCoercion, typeManager)) {
            return false;
        }

        return matchArguments(boundParameters, unboundParameters, argumentTypes, types, allowCoercion, variableArity, typeManager);
    }

    public boolean match(List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        Map<String, TypeParameter> unboundParameters = new HashMap<>();
        for (TypeParameter parameter : typeParameters) {
            unboundParameters.put(parameter.getName(), parameter);
        }

        return matchArguments(boundParameters, unboundParameters, argumentTypes, types, allowCoercion, variableArity, typeManager);
    }

    private static boolean matchArguments(
            Map<String, Type> boundParameters,
            Map<String, TypeParameter> unboundParameters,
            List<TypeSignature> argumentTypes,
            List<? extends Type> types,
            boolean allowCoercion,
            boolean varArgs,
            TypeManager typeManager)
    {
        if (varArgs) {
            if (types.size() < argumentTypes.size() - 1) {
                return false;
            }
        }
        else {
            if (argumentTypes.size() != types.size()) {
                return false;
            }
        }

        for (int i = 0; i < types.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature typeSignature = argumentTypes.get(Math.min(i, argumentTypes.size() - 1));
            Type type = types.get(i);
            if (!matchAndBind(boundParameters, unboundParameters, typeSignature, type, allowCoercion, typeManager)) {
                return false;
            }
        }

        return true;
    }

    private static boolean matchAndBind(Map<String, Type> boundParameters, Map<String, TypeParameter> unboundParameters, TypeSignature signature, Type type, boolean allowCoercion, TypeManager typeManager)
    {
        // If this parameter is already bound, then match (with coercion)
        if (boundParameters.containsKey(signature.getBase())) {
            checkArgument(signature.getParameters().isEmpty(), "Unexpected parameteric type");
            if (allowCoercion) {
                return canCoerce(type, boundParameters.get(signature.getBase()));
            }
            else {
                return type.equals(boundParameters.get(signature.getBase()));
            }
        }

        // Bind parameter, if this is a free type parameter
        if (unboundParameters.containsKey(signature.getBase())) {
            TypeParameter typeParameter = unboundParameters.get(signature.getBase());
            if (!typeParameter.canBind(type)) {
                return false;
            }
            unboundParameters.remove(signature.getBase());
            boundParameters.put(signature.getBase(), type);
            return true;
        }

        if (allowCoercion) {
            return canCoerce(type, typeManager.getType(signature.getBase()));
        }
        else {
            return type.equals(typeManager.getType(signature.getBase()));
        }
    }

    public static TypeParameter typeParameter(String name)
    {
        return new TypeParameter(name, false);
    }

    public static TypeParameter typeParameter(String name, boolean comparableRequired)
    {
        return new TypeParameter(name, comparableRequired);
    }

    public static final class TypeParameter
    {
        private final String name;
        private final boolean comparableRequired;

        @JsonCreator
        public TypeParameter(@JsonProperty("name") String name, @JsonProperty("comparableRequired") boolean comparableRequired)
        {
            this.name = checkNotNull(name, "name is null");
            this.comparableRequired = comparableRequired;
        }

        @JsonProperty
        public String getName()
        {
            return name;
        }

        @JsonProperty
        public boolean isComparableRequired()
        {
            return comparableRequired;
        }

        public boolean canBind(Type type)
        {
            return !comparableRequired || type.isComparable();
        }

        @Override
        public String toString()
        {
            return comparableRequired ? name + ":comparable" : name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            TypeParameter other = (TypeParameter) o;

            return Objects.equals(this.name, other.name) &&
                    Objects.equals(this.comparableRequired, other.comparableRequired);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(name, comparableRequired);
        }
    }
}
