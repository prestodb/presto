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
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.any;
import static java.lang.String.format;
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

    public Signature(
            String name,
            FunctionKind kind,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            String returnType,
            List<String> argumentTypes,
            boolean variableArity)
    {
        this(name, kind, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity, ImmutableSet.of());
    }

    public Signature(
            String name,
            FunctionKind kind,
            List<TypeVariableConstraint> typeVariableConstraints,
            List<LongVariableConstraint> longVariableConstraints,
            String returnType,
            List<String> argumentTypes,
            boolean variableArity,
            Set<String> literalParameters)
    {
        this(name,
                kind,
                typeVariableConstraints,
                longVariableConstraints,
                parseTypeSignature(returnType, literalParameters),
                argumentTypes.stream().map(argument -> parseTypeSignature(argument, literalParameters)).collect(toImmutableList()),
                variableArity
        );
    }

    public Signature(String name, FunctionKind kind, String returnType, List<String> argumentTypes)
    {
        this(name,
                kind,
                ImmutableList.<TypeVariableConstraint>of(),
                ImmutableList.<LongVariableConstraint>of(),
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
        this(name, kind, ImmutableList.<TypeVariableConstraint>of(), ImmutableList.<LongVariableConstraint>of(), returnType, argumentTypes, false);
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
        return new Signature(name, SCALAR, ImmutableList.<TypeVariableConstraint>of(), ImmutableList.<LongVariableConstraint>of(), returnType, argumentTypes, false, ImmutableSet.of());
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, TypeSignature... argumentTypes)
    {
        return internalScalarFunction(name, returnType, ImmutableList.copyOf(argumentTypes));
    }

    public static Signature internalScalarFunction(String name, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return new Signature(name, SCALAR, ImmutableList.<TypeVariableConstraint>of(), ImmutableList.<LongVariableConstraint>of(), returnType, argumentTypes, false);
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

    public Signature resolveCalculatedTypes(List<TypeSignature> parameterTypes)
    {
        if (!isReturnTypeOrAnyArgumentTypeCalculated()) {
            return this;
        }

        Map<String, OptionalLong> inputs = bindLongVariables(parameterTypes);
        TypeSignature calculatedReturnType = TypeUtils.resolveCalculatedType(returnType, inputs);
        return new Signature(
                name,
                kind,
                calculatedReturnType,
                argumentTypes.stream().map(parameter -> TypeUtils.resolveCalculatedType(parameter, inputs)).collect(toImmutableList()));
    }

    public Map<String, OptionalLong> bindLongVariables(List<TypeSignature> parameterTypes)
    {
        if (!isReturnTypeOrAnyArgumentTypeCalculated()) {
            return ImmutableMap.of();
        }

        Map<String, OptionalLong> boundVariables = bindLongVariablesFromParameterTypes(parameterTypes);
        Map<String, OptionalLong> calculatedVariables = calculateVariablesValuesForLongConstraints(boundVariables);

        return ImmutableMap.<String, OptionalLong>builder()
                .putAll(boundVariables)
                .putAll(calculatedVariables).build();
    }

    private Map<String, OptionalLong> bindLongVariablesFromParameterTypes(List<TypeSignature> parameterTypes)
    {
        parameterTypes = replaceSameArgumentsWithCommonSuperType(parameterTypes);

        Map<String, OptionalLong> boundVariables = new HashMap<>();
        for (int index = 0; index < argumentTypes.size(); index++) {
            TypeSignature argument = argumentTypes.get(index);
            if (argument.isCalculated()) {
                TypeSignature actualParameter = parameterTypes.get(index);

                Map<String, OptionalLong> matchedLiterals = TypeUtils.computeParameterBindings(argument, actualParameter);
                for (String literal : matchedLiterals.keySet()) {
                    OptionalLong value = matchedLiterals.get(literal);
                    checkArgument(
                            boundVariables.getOrDefault(literal, value).equals(value),
                            "Literal [%s] with value [%s] for argument [%s] has been previously matched to different value [%s]",
                            literal,
                            value,
                            argument,
                            boundVariables.get(literal));
                }
                boundVariables.putAll(matchedLiterals);
            }
        }
        return boundVariables;
    }

    private Map<String, OptionalLong> calculateVariablesValuesForLongConstraints(Map<String, OptionalLong> inputs)
    {
        return longVariableConstraints.stream()
                .collect(Collectors.toMap(
                        c -> c.getName().toUpperCase(Locale.US),
                        c -> calculateLiteralValue(c.getExpression(), inputs, true)
                ));
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
        return Objects.hash(name, kind, typeVariableConstraints, longVariableConstraints, returnType, argumentTypes, variableArity);
    }

    Signature withAlias(String name)
    {
        return new Signature(name, kind, typeVariableConstraints, longVariableConstraints, getReturnType(), getArgumentTypes(), variableArity);
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

    @Nullable
    public Map<String, Type> bindTypeVariables(Type returnType, List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        Map<String, TypeVariableConstraint> constraints = getTypeVariableConstraintsAsMap();
        if (!matchAndBind(boundParameters, constraints, this.returnType, returnType, allowCoercion, typeManager)) {
            return null;
        }

        if (!matchArguments(boundParameters, constraints, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }

        checkState(boundParameters.keySet().equals(constraints.keySet()),
                "%s matched arguments %s, but type constraints %s are still unbound",
                this,
                types,
                Sets.difference(constraints.keySet(), boundParameters.keySet()));

        return boundParameters;
    }

    public Map<String, Type> bindTypeVariables(List<? extends Type> types, boolean allowCoercion, TypeManager typeManager)
    {
        Map<String, Type> boundParameters = new HashMap<>();
        Map<String, TypeVariableConstraint> constraints = getTypeVariableConstraintsAsMap();
        if (!matchArguments(boundParameters, constraints, argumentTypes, types, allowCoercion, variableArity, typeManager)) {
            return null;
        }
        checkState(boundParameters.keySet().equals(constraints.keySet()), "%s matched arguments %s, but type constraints %s are still unbound", this, types, Sets.difference(constraints.keySet(), boundParameters.keySet()));
        return boundParameters;
    }

    private Map<String, TypeVariableConstraint> getTypeVariableConstraintsAsMap()
    {
        ImmutableMap.Builder<String, TypeVariableConstraint> builder = ImmutableMap.builder();
        for (TypeVariableConstraint parameter : typeVariableConstraints) {
            builder.put(parameter.getName(), parameter);
        }
        return builder.build();
    }

    private boolean isReturnTypeOrAnyArgumentTypeCalculated()
    {
        return returnType.isCalculated() || any(argumentTypes, TypeSignature::isCalculated);
    }

    private static boolean matchArguments(
            Map<String, Type> boundParameters,
            Map<String, TypeVariableConstraint> typeVaraibleConstraints,
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
            if (!matchAndBind(boundParameters, typeVaraibleConstraints, argumentTypes.get(argumentTypes.size() - 1), superType.get(), allowCoercion, typeManager)) {
                return false;
            }
        }

        for (int i = 0; i < types.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature typeSignature = argumentTypes.get(Math.min(i, argumentTypes.size() - 1));
            Type type = types.get(i);
            if (!matchAndBind(boundParameters, typeVaraibleConstraints, typeSignature, type, allowCoercion, typeManager)) {
                return false;
            }
        }

        return true;
    }

    private static boolean matchAndBind(Map<String, Type> boundParameters, Map<String, TypeVariableConstraint> typeVariableConstraints, TypeSignature parameter, Type type, boolean allowCoercion, TypeManager typeManager)
    {
        List<TypeSignatureParameter> parameters = parameter.getParameters();

        // If this parameter is already bound, then match (with coercion)
        if (boundParameters.containsKey(parameter.getBase())) {
            checkArgument(parameter.getParameters().isEmpty(), "Unexpected parametric type");
            if (allowCoercion && !parameter.isCalculated()) {
                if (canCoerce(type, boundParameters.get(parameter.getBase()))) {
                    return true;
                }
                else if (canCoerce(boundParameters.get(parameter.getBase()), type) && typeVariableConstraints.get(parameter.getBase()).canBind(type)) {
                    // Try to coerce current binding to new candidate
                    boundParameters.put(parameter.getBase(), type);
                    return true;
                }
                else {
                    // Try to use common super type of current binding and candidate
                    Optional<Type> commonSuperType = typeManager.getCommonSuperType(boundParameters.get(parameter.getBase()), type);
                    if (commonSuperType.isPresent() && typeVariableConstraints.get(parameter.getBase()).canBind(commonSuperType.get())) {
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
                if (!matchAndBindParameters(boundParameters, typeVariableConstraints, parameter, type, allowCoercion, typeManager)) {
                    parametersMatched = false;
                }
            }
            else {
                parametersMatched = false;
            }
        }

        // Bind parameter, if this is a free type parameter
        if (typeVariableConstraints.containsKey(parameter.getBase())) {
            TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(parameter.getBase());
            if (!typeVariableConstraint.canBind(type)) {
                return false;
            }
            boundParameters.put(parameter.getBase(), type);
            return true;
        }

        // If parameters don't match, and base type differs
        if (!parametersMatched && !typeSignature.getBase().equals(parameter.getBase())) {
            // check for possible coercion
            if (allowCoercion && canCoerce(typeSignature, parameter)) {
                Type coercedType = typeManager.getType(TypeRegistry.getUnmatchedSignature(parameter));
                if (coercedType == null) {
                    return false;
                }

                return matchAndBind(
                        boundParameters,
                        typeVariableConstraints,
                        parameter,
                        coercedType,
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
            Map<String, TypeVariableConstraint> typeVariableConstraints,
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

            if (componentParameter.isVariable()) {
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
                if (!matchAndBind(boundParameters, typeVariableConstraints, componentSignature, componentType, allowCoercion, typeManager)) {
                    return false;
                }
            }
        }
        return true;
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
