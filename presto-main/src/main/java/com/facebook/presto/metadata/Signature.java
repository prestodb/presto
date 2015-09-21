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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.metadata.FunctionRegistry.canCoerce;
import static com.facebook.presto.metadata.FunctionRegistry.getCommonSuperType;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.FunctionType.SCALAR;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class Signature
{
    private final String name;
    private final FunctionType type;
    private final List<TypeParameter> typeParameters;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    @JsonCreator
    public Signature(
            @JsonProperty("name") String name,
            @JsonProperty("type") FunctionType type,
            @JsonProperty("typeParameters") List<TypeParameter> typeParameters,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeParameters, "typeParameters is null");

        this.name = name;
        this.type = requireNonNull(type, "type is null");
        this.typeParameters = ImmutableList.copyOf(typeParameters);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.variableArity = variableArity;
    }

    public Signature(String name, FunctionType type, List<TypeParameter> typeParameters, String returnType, List<String> argumentTypes, boolean variableArity)
    {
        this(name, type, typeParameters, parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), variableArity);
    }

    public Signature(String name, FunctionType type, String returnType, List<String> argumentTypes)
    {
        this(name, type, ImmutableList.<TypeParameter>of(), parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), false);
    }

    public Signature(String name, FunctionType type, String returnType, String... argumentTypes)
    {
        this(name, type, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public Signature(String name, FunctionType type, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        this(name, type, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false);
    }

    public Signature(String name, FunctionType type, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        this(name, type, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends  Type> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(toImmutableList()));
    }

    public static Signature internalOperator(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, argumentTypes);
    }

    public static Signature internalOperator(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(name), returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, String returnType, String... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, String returnType, List<String> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false);
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeParameter>of(), returnType, argumentTypes, false);
    }

    @JsonProperty
    public String getName()
    {
        return name;
    }

    @JsonProperty
    public FunctionType getType()
    {
        return type;
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
    public List<TypeParameter> getTypeParameters()
    {
        return typeParameters;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, type, typeParameters, returnType, argumentTypes, variableArity);
    }

    Signature withAlias(String name)
    {
        return new Signature(name, type, typeParameters, getReturnType(), getArgumentTypes(), variableArity);
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
                Objects.equals(this.type, other.type) &&
                Objects.equals(this.typeParameters, other.typeParameters) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity);
    }

    @Override
    public String toString()
    {
        return name + (typeParameters.isEmpty() ? "" : "<" + Joiner.on(",").join(typeParameters) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(Type returnType, List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        ImmutableMap.Builder<String, TypeParameter> builder = ImmutableMap.builder();
        for (TypeParameter parameter : typeParameters) {
            builder.put(parameter.getName(), parameter);
        }

        ImmutableMap<String, TypeParameter> parameters = builder.build();
        if (!matchAndBind(boundParameters, parameters, this.returnType, returnType, allowCoercion, typeManager)) {
            return null;
        }

        if (!matchArguments(boundParameters, parameters, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }

        checkState(boundParameters.keySet().equals(parameters.keySet()),
                "%s matched arguments %s, but type parameters %s are still unbound",
                this,
                types,
                Sets.difference(parameters.keySet(), boundParameters.keySet()));

        return boundParameters;
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        ImmutableMap.Builder<String, TypeParameter> builder = ImmutableMap.builder();
        for (TypeParameter parameter : typeParameters) {
            builder.put(parameter.getName(), parameter);
        }

        ImmutableMap<String, TypeParameter> parameters = builder.build();
        if (!matchArguments(boundParameters, parameters, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }

        checkState(boundParameters.keySet().equals(parameters.keySet()), "%s matched arguments %s, but type parameters %s are still unbound", this, types, Sets.difference(parameters.keySet(), boundParameters.keySet()));

        return boundParameters;
    }

    private static boolean matchArguments(
            Map<String, Type> boundParameters,
            Map<String, TypeParameter> parameters,
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

        // Bind the variable arity argument first, to make sure it's bound to the common super type
        if (varArgs && types.size() >= argumentTypes.size()) {
            Optional<Type> superType = getCommonSuperType(types.subList(argumentTypes.size() - 1, types.size()));
            if (!superType.isPresent()) {
                return false;
            }
            if (!matchAndBind(boundParameters, parameters, argumentTypes.get(argumentTypes.size() - 1), superType.get(), allowCoercion, typeManager)) {
                return false;
            }
        }

        for (int i = 0; i < types.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature typeSignature = argumentTypes.get(Math.min(i, argumentTypes.size() - 1));
            Type type = types.get(i);
            if (!matchAndBind(boundParameters, parameters, typeSignature, type, allowCoercion, typeManager)) {
                return false;
            }
        }

        return true;
    }

    private static boolean matchAndBind(Map<String, Type> boundParameters, Map<String, TypeParameter> typeParameters, TypeSignature parameter, Type type, boolean allowCoercion, TypeManager typeManager)
    {
        // If this parameter is already bound, then match (with coercion)
        if (boundParameters.containsKey(parameter.getBase())) {
            checkArgument(parameter.getParameters().isEmpty(), "Unexpected parameteric type");
            if (allowCoercion) {
                if (canCoerce(type, boundParameters.get(parameter.getBase()))) {
                    return true;
                }
                else if (canCoerce(boundParameters.get(parameter.getBase()), type) && typeParameters.get(parameter.getBase()).canBind(type)) {
                    // Broaden the binding
                    boundParameters.put(parameter.getBase(), type);
                    return true;
                }
                return false;
            }
            else {
                return type.equals(boundParameters.get(parameter.getBase()));
            }
        }

        // Recurse into component types
        if (!parameter.getParameters().isEmpty()) {
            if (type.getTypeParameters().size() != parameter.getParameters().size()) {
                return false;
            }
            for (int i = 0; i < parameter.getParameters().size(); i++) {
                Type componentType = type.getTypeParameters().get(i);
                TypeSignature componentSignature = parameter.getParameters().get(i);
                if (!matchAndBind(boundParameters, typeParameters, componentSignature, componentType, allowCoercion, typeManager)) {
                    return false;
                }
            }
        }

        // Bind parameter, if this is a free type parameter
        if (typeParameters.containsKey(parameter.getBase())) {
            TypeParameter typeParameter = typeParameters.get(parameter.getBase());
            if (!typeParameter.canBind(type)) {
                return false;
            }
            boundParameters.put(parameter.getBase(), type);
            return true;
        }

        // We've already checked all the components, so just match the base type
        if (!parameter.getParameters().isEmpty()) {
            return type.getTypeSignature().getBase().equals(parameter.getBase());
        }

        // The parameter is not a type parameter, so it must be a concrete type
        if (allowCoercion) {
            return canCoerce(type, typeManager.getType(parseTypeSignature(parameter.getBase())));
        }
        else {
            return type.equals(typeManager.getType(parseTypeSignature(parameter.getBase())));
        }
    }

    /*
     * similar to T extends MyClass<?...>, if Java supported varargs wildcards
     */
    public static TypeParameter withVariadicBound(String name, String variadicBound)
    {
        return new TypeParameter(name, false, false, variadicBound);
    }

    public static TypeParameter comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeParameter(name, true, false, variadicBound);
    }

    public static TypeParameter typeParameter(String name)
    {
        return new TypeParameter(name, false, false, null);
    }

    public static TypeParameter comparableTypeParameter(String name)
    {
        return new TypeParameter(name, true, false, null);
    }

    public static TypeParameter orderableTypeParameter(String name)
    {
        return new TypeParameter(name, false, true, null);
    }
}
