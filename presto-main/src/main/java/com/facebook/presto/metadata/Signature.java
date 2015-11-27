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
import com.facebook.presto.spi.type.TypeSignatureParameter;
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

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public final class Signature
{
    private final String name;
    private final FunctionKind kind;
    private final List<TypeParameterRequirement> typeParameterRequirements;
    private final TypeSignature returnType;
    private final List<TypeSignature> argumentTypes;
    private final boolean variableArity;

    @JsonCreator
    public Signature(
            @JsonProperty("name") String name,
            @JsonProperty("kind") FunctionKind kind,
            @JsonProperty("typeParameterRequirements") List<TypeParameterRequirement> typeParameterRequirements,
            @JsonProperty("returnType") TypeSignature returnType,
            @JsonProperty("argumentTypes") List<TypeSignature> argumentTypes,
            @JsonProperty("variableArity") boolean variableArity)
    {
        requireNonNull(name, "name is null");
        requireNonNull(typeParameterRequirements, "typeParameters is null");

        this.name = name;
        this.kind = requireNonNull(kind, "type is null");
        this.typeParameterRequirements = ImmutableList.copyOf(typeParameterRequirements);
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
        this.variableArity = variableArity;
    }

    public Signature(String name, FunctionKind kind, List<TypeParameterRequirement> typeParameterRequirements, String returnType, List<String> argumentTypes, boolean variableArity)
    {
        this(name, kind, typeParameterRequirements, parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), variableArity);
    }

    public Signature(String name, FunctionKind kind, String returnType, List<String> argumentTypes)
    {
        this(name, kind, ImmutableList.<TypeParameterRequirement>of(), parseTypeSignature(returnType), Lists.transform(argumentTypes, TypeSignature::parseTypeSignature), false);
    }

    public Signature(String name, FunctionKind kind, String returnType, String... argumentTypes)
    {
        this(name, kind, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public Signature(String name, FunctionKind kind, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        this(name, kind, ImmutableList.<TypeParameterRequirement>of(), returnType, argumentTypes, false);
    }

    public Signature(String name, FunctionKind kind, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        this(name, kind, returnType, ImmutableList.copyOf(argumentTypes));
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
        return new Signature(name, SCALAR, ImmutableList.<TypeParameterRequirement>of(), returnType, argumentTypes, false);
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeParameterRequirement>of(), returnType, argumentTypes, false);
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
    public List<TypeParameterRequirement> getTypeParameterRequirements()
    {
        return typeParameterRequirements;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(name, kind, typeParameterRequirements, returnType, argumentTypes, variableArity);
    }

    Signature withAlias(String name)
    {
        return new Signature(name, kind, typeParameterRequirements, getReturnType(), getArgumentTypes(), variableArity);
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
                Objects.equals(this.typeParameterRequirements, other.typeParameterRequirements) &&
                Objects.equals(this.returnType, other.returnType) &&
                Objects.equals(this.argumentTypes, other.argumentTypes) &&
                Objects.equals(this.variableArity, other.variableArity);
    }

    @Override
    public String toString()
    {
        return name + (typeParameterRequirements.isEmpty() ? "" : "<" + Joiner.on(",").join(typeParameterRequirements) + ">") + "(" + Joiner.on(",").join(argumentTypes) + "):" + returnType;
    }

    @Nullable
    public Map<String, Type> bindTypeParameters(Type returnType, List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        ImmutableMap.Builder<String, TypeParameterRequirement> builder = ImmutableMap.builder();
        for (TypeParameterRequirement parameter : typeParameterRequirements) {
            builder.put(parameter.getName(), parameter);
        }

        ImmutableMap<String, TypeParameterRequirement> parameters = builder.build();
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
        ImmutableMap.Builder<String, TypeParameterRequirement> builder = ImmutableMap.builder();
        for (TypeParameterRequirement parameter : typeParameterRequirements) {
            builder.put(parameter.getName(), parameter);
        }

        ImmutableMap<String, TypeParameterRequirement> parameters = builder.build();
        if (!matchArguments(boundParameters, parameters, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }

        checkState(boundParameters.keySet().equals(parameters.keySet()), "%s matched arguments %s, but type parameters %s are still unbound", this, types, Sets.difference(parameters.keySet(), boundParameters.keySet()));

        return boundParameters;
    }

    private static boolean matchArguments(
            Map<String, Type> boundParameters,
            Map<String, TypeParameterRequirement> parameters,
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
            Optional<Type> superType = typeManager.getCommonSuperType(types.subList(argumentTypes.size() - 1, types.size()));
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

    private static boolean matchAndBind(Map<String, Type> boundParameters, Map<String, TypeParameterRequirement> typeParameters, TypeSignature parameter, Type type, boolean allowCoercion, TypeManager typeManager)
    {
        List<TypeSignatureParameter> parameters = parameter.getParameters();
        // If this parameter is already bound, then match (with coercion)
        if (boundParameters.containsKey(parameter.getBase())) {
            checkArgument(parameters.isEmpty(), "Unexpected parameteric type");
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
        if (!parameters.isEmpty()) {
            // TODO: add support for types with both literal and type parameters? Now for such types this check will fail
            if (type.getTypeParameters().size() != parameters.size()) {
                return false;
            }
            for (int i = 0; i < parameters.size(); i++) {
                Type componentType = type.getTypeParameters().get(i);
                TypeSignatureParameter componentParameter = parameters.get(i);
                TypeSignature componentSignature;
                switch (componentParameter.getKind()) {
                    case TYPE_SIGNATURE:
                        componentSignature = componentParameter.getTypeSignature();
                        break;
                    case NAMED_TYPE_SIGNATURE:
                        componentSignature = componentParameter.getNamedTypeSignature().getTypeSignature();
                        break;
                    default:
                        // TODO: add support for types with both literal and type parameters?
                        return false;
                }

                if (!matchAndBind(boundParameters, typeParameters, componentSignature, componentType, allowCoercion, typeManager)) {
                    return false;
                }
            }
        }

        // Bind parameter, if this is a free type parameter
        if (typeParameters.containsKey(parameter.getBase())) {
            TypeParameterRequirement typeParameterRequirement = typeParameters.get(parameter.getBase());
            if (!typeParameterRequirement.canBind(type)) {
                return false;
            }
            boundParameters.put(parameter.getBase(), type);
            return true;
        }

        // We've already checked all the components, so just match the base type
        if (!parameters.isEmpty()) {
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
    public static TypeParameterRequirement withVariadicBound(String name, String variadicBound)
    {
        return new TypeParameterRequirement(name, false, false, variadicBound);
    }

    public static TypeParameterRequirement comparableWithVariadicBound(String name, String variadicBound)
    {
        return new TypeParameterRequirement(name, true, false, variadicBound);
    }

    public static TypeParameterRequirement typeParameter(String name)
    {
        return new TypeParameterRequirement(name, false, false, null);
    }

    public static TypeParameterRequirement comparableTypeParameter(String name)
    {
        return new TypeParameterRequirement(name, true, false, null);
    }

    public static TypeParameterRequirement orderableTypeParameter(String name)
    {
        return new TypeParameterRequirement(name, false, true, null);
    }
}
