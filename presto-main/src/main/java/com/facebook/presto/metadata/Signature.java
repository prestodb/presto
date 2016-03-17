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
import com.facebook.presto.type.TypeRegistry;
import com.facebook.presto.type.TypeUtils;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.any;
import static java.lang.String.format;
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

    public Signature(
            String name,
            FunctionKind kind,
            List<TypeParameterRequirement> typeParameterRequirements,
            String returnType,
            List<String> argumentTypes,
            boolean variableArity)
    {
        this(name, kind, typeParameterRequirements, returnType, argumentTypes, variableArity, ImmutableSet.of());
    }

    public Signature(
            String name,
            FunctionKind kind,
            List<TypeParameterRequirement> typeParameterRequirements,
            String returnType,
            List<String> argumentTypes,
            boolean variableArity,
            Set<String> literalParameters)
    {
        this(name,
                kind,
                typeParameterRequirements,
                parseTypeSignature(returnType, literalParameters),
                argumentTypes.stream().map(argument -> parseTypeSignature(argument, literalParameters)).collect(toImmutableList()),
                variableArity
        );
    }

    public Signature(String name, FunctionKind kind, String returnType, List<String> argumentTypes)
    {
        this(name,
                kind,
                ImmutableList.<TypeParameterRequirement>of(),
                parseTypeSignature(returnType),
                argumentTypes.stream().map(TypeSignature::parseTypeSignature).collect(toImmutableList()),
                false
        );
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

    public static Signature internalOperator(OperatorType operator, Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(mangleOperatorName(operator.name()), returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(toImmutableList()));
    }

    public static Signature internalOperator(OperatorType operator, String returnType, List<String> argumentTypes)
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

    public static Signature internalScalarFunction(String name, String returnType, String... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, String returnType, List<String> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeParameterRequirement>of(), returnType, argumentTypes, false, ImmutableSet.of());
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

    public Signature resolveCalculatedTypes(List<TypeSignature> parameterTypes)
    {
        if (!isReturnTypeOrAnyArgumentTypeCalculated()) {
            return this;
        }

        Map<String, OptionalLong> inputs = bindLiteralParameters(parameterTypes);
        TypeSignature calculatedReturnType = TypeUtils.resolveCalculatedType(returnType, inputs, true);
        return new Signature(
                name,
                kind,
                calculatedReturnType,
                argumentTypes.stream().map(parameter -> TypeUtils.resolveCalculatedType(parameter, inputs, false)).collect(toImmutableList()));
    }

    public Map<String, OptionalLong> bindLiteralParameters(List<TypeSignature> parameterTypes)
    {
        parameterTypes = replaceSameArgumentsWithCommonSuperType(parameterTypes);

        Map<String, OptionalLong> boundParameters = new HashMap<>();
        for (int index = 0; index < argumentTypes.size(); index++) {
            TypeSignature argument = argumentTypes.get(index);
            if (argument.isCalculated()) {
                TypeSignature actualParameter = parameterTypes.get(index);

                Map<String, OptionalLong> matchedLiterals = TypeUtils.computeParameterBindings(argument, actualParameter);
                for (String literal : matchedLiterals.keySet()) {
                    OptionalLong value = matchedLiterals.get(literal);
                    checkArgument(
                            boundParameters.getOrDefault(literal, value).equals(value),
                            "Literal [%s] with value [%s] for argument [%s] has been previously matched to different value [%s]",
                            literal,
                            value,
                            argument,
                            boundParameters.get(literal));
                }
                boundParameters.putAll(matchedLiterals);
            }
        }
        return boundParameters;
    }

    private List<TypeSignature> replaceSameArgumentsWithCommonSuperType(List<TypeSignature> parameters)
    {
        checkArgument(parameters.size() == argumentTypes.size(), "Wrong number of parameters");

        Map<TypeSignature, TypeSignature> commonSuperTypes = new HashMap<>();
        for (int index = 0; index < argumentTypes.size(); index++) {
            TypeSignature argument = argumentTypes.get(index);
            TypeSignature parameter = parameters.get(index);

            if (!commonSuperTypes.containsKey(argument)) {
                commonSuperTypes.put(argument, parameter);
            }
            else {
                TypeSignature otherParameter = commonSuperTypes.get(argument);
                Optional<TypeSignature> commonSuperParameter = getCommonSuperTypeSignature(parameter, otherParameter);
                checkArgument(
                        commonSuperParameter.isPresent(),
                        "Parameters [%s] and [%s] must match to same signature [%s] but can not be coerced",
                        otherParameter,
                        parameter,
                        argument);
                commonSuperTypes.put(argument, commonSuperParameter.get());
            }
        }
        return argumentTypes.stream().map(commonSuperTypes::get).collect(toImmutableList());
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

    private boolean isReturnTypeOrAnyArgumentTypeCalculated()
    {
        return returnType.isCalculated() || any(argumentTypes, TypeSignature::isCalculated);
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
            checkArgument(parameter.getParameters().isEmpty(), "Unexpected parametric type");
            if (allowCoercion && !parameter.isCalculated()) {
                if (canCoerce(type, boundParameters.get(parameter.getBase()))) {
                    return true;
                }
                else if (canCoerce(boundParameters.get(parameter.getBase()), type) && typeParameters.get(parameter.getBase()).canBind(type)) {
                    // Try to coerce current binding to new candidate
                    boundParameters.put(parameter.getBase(), type);
                    return true;
                }
                else {
                    // Try to use common super type of current binding and candidate
                    Optional<Type> commonSuperType = typeManager.getCommonSuperType(boundParameters.get(parameter.getBase()), type);
                    if (commonSuperType.isPresent() && typeParameters.get(parameter.getBase()).canBind(commonSuperType.get())) {
                        boundParameters.put(parameter.getBase(), commonSuperType.get());
                        return true;
                    }
                }
                return false;
            }
            else {
                return type.equals(boundParameters.get(parameter.getBase()));
            }
        }

        TypeSignature typeSignature = type.getTypeSignature();

        boolean parametersMatched = true;
        // Recurse into component types
        if (!parameters.isEmpty()) {
            // TODO: add support for types with both literal and type parameters? Now for such types this check will fail
            if (typeSignature.getParameters().size() == parameters.size()) {
                if (!matchAndBindParameters(boundParameters, typeParameters, parameter, type, allowCoercion, typeManager)) {
                    parametersMatched = false;
                }
            }
            else {
                parametersMatched = false;
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

        // If parameters don't match, and base type differs
        if (!parametersMatched && !typeSignature.getBase().equals(parameter.getBase())) {
            // check for possible coercion
            if (allowCoercion && canCoerce(typeSignature, parameter)) {
                return matchAndBind(
                        boundParameters,
                        typeParameters,
                        parameter,
                        requireNonNull(typeManager.getType(TypeRegistry.getUnmatchedSignature(parameter))),
                        true,
                        typeManager);
            }
            else {
                return false;
            }
        }

        // We've already checked all the components, so just match the base type
        if (parametersMatched && !parameters.isEmpty()) {
            return typeSignature.getBase().equals(parameter.getBase());
        }

        // The parameter is not a type parameter, so try if it's a concrete type and can coerce
        Type parameterType = typeManager.getType(parameter);
        if (parameterType == null) {
            return false;
        }

        if (allowCoercion && !parameter.isCalculated()) {
            return canCoerce(type, parameterType);
        }
        else if (parametersMatched) {
            return typeSignature.getBase().equals(parameterType.getTypeSignature().getBase());
        }

        return false;
    }

    private static boolean matchAndBindParameters(
            Map<String, Type> boundParameters,
            Map<String, TypeParameterRequirement> typeParameters,
            TypeSignature parameter,
            Type type,
            boolean allowCoercion,
            TypeManager typeManager)
    {
        // TODO: add support for mixed literal and type parameters. At the moment index of type.getTypeParameters().get(i) will not match parameters' index
        List<TypeSignatureParameter> parameters = parameter.getParameters();
        for (int i = 0; i < parameters.size(); i++) {
            TypeSignatureParameter actualTypeParameter = type.getTypeSignature().getParameters().get(i);
            TypeSignatureParameter typeSignatureParameter = type.getTypeSignature().getParameters().get(i);
            TypeSignatureParameter componentParameter = parameters.get(i);

            if (componentParameter.isLiteralCalculation()) {
                if (!typeSignatureParameter.isLongLiteral()) {
                    return false;
                }
            }
            else if (componentParameter.isLongLiteral()) {
                if (!typeSignatureParameter.isLongLiteral()) {
                    return false;
                }
                if (componentParameter.getLongLiteral().longValue() != typeSignatureParameter.getLongLiteral().longValue()) {
                    return false;
                }
            }
            else {
                TypeSignature componentSignature;
                if (componentParameter.isTypeSignature()) {
                    if (!actualTypeParameter.isTypeSignature() && !actualTypeParameter.isNamedTypeSignature()) {
                        return false;
                    }
                    componentSignature = componentParameter.getTypeSignature();
                }
                else if (componentParameter.isNamedTypeSignature() && actualTypeParameter.isNamedTypeSignature()) {
                    componentSignature = componentParameter.getNamedTypeSignature().getTypeSignature();
                }
                else {
                    throw new UnsupportedOperationException(format("Unsupported TypeSignatureParameter [%s]", componentParameter));
                }

                Type componentType = type.getTypeParameters().get(i);
                if (!matchAndBind(boundParameters, typeParameters, componentSignature, componentType, allowCoercion, typeManager)) {
                    return false;
                }
            }
        }
        return true;
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

    public static SignatureBuilder builder()
    {
        return new SignatureBuilder();
    }
}
