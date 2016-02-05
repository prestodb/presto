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

import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.ParameterKind;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.TypeSignatureParameter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
import static com.facebook.presto.type.TypeRegistry.canCoerce;
import static com.facebook.presto.type.TypeRegistry.getCommonSuperTypeSignature;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getLast;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class SignatureBinder
{
    private final TypeManager typeManager;
    private final Signature declaredSignature;
    private final boolean allowCoercion;
    private final Map<String, TypeVariableConstraint> typeVariableConstraints;

    public SignatureBinder(TypeManager typeManager, Signature declaredSignature, boolean allowCoercion)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.declaredSignature = requireNonNull(declaredSignature, "parametrizedSignature is null");
        this.allowCoercion = allowCoercion;
        this.typeVariableConstraints = declaredSignature.getTypeVariableConstraints()
                .stream()
                .collect(toMap(TypeVariableConstraint::getName, t -> t));
    }

    public Optional<Signature> matchAndBindSignature(List<? extends Type> actualArgumentTypes)
    {
        Optional<BoundVariables> boundVariables = matchAndBindSignatureVariables(actualArgumentTypes);
        if (!boundVariables.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(bindVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    public Optional<BoundVariables> matchAndBindSignatureVariables(List<? extends Type> actualArgumentTypes)
    {
        List<TypeSignature> expectedArgumentSignatures = declaredSignature.getArgumentTypes();
        boolean variableArity = declaredSignature.isVariableArity();
        BoundVariables.Builder boundVariablesBuilder = BoundVariables.builder();
        if (!matchArguments(expectedArgumentSignatures, actualArgumentTypes, variableArity, boundVariablesBuilder)) {
            return Optional.empty();
        }
        calculateVariableValuesForLongConstraints(boundVariablesBuilder);
        BoundVariables boundVariables = boundVariablesBuilder.build();
        checkAllTypeVariablesAreBound(boundVariables);
        return Optional.of(boundVariables);
    }

    public Optional<BoundVariables> matchAndBindSignatureVariables(List<? extends Type> actualArgumentTypes, Type actualReturnType)
    {
        BoundVariables.Builder boundVariablesBuilder = BoundVariables.builder();

        TypeSignature expectedReturnTypeSignature = declaredSignature.getReturnType();
        if (!matchAndBindArgument(expectedReturnTypeSignature, actualReturnType, boundVariablesBuilder)) {
            return Optional.empty();
        }

        List<TypeSignature> expectedArgumentSignatures = declaredSignature.getArgumentTypes();
        boolean variableArity = declaredSignature.isVariableArity();
        if (!matchArguments(expectedArgumentSignatures, actualArgumentTypes, variableArity, boundVariablesBuilder)) {
            return Optional.empty();
        }

        calculateVariableValuesForLongConstraints(boundVariablesBuilder);

        BoundVariables boundVariables = boundVariablesBuilder.build();
        checkAllTypeVariablesAreBound(boundVariables);
        return Optional.of(boundVariables);
    }

    private boolean matchArguments(
            List<TypeSignature> expectedArgumentSignatures,
            List<? extends Type> actualArgumentTypes,
            boolean variableArity,
            BoundVariables.Builder boundVariables)
    {
        if (variableArity) {
            if (actualArgumentTypes.size() < expectedArgumentSignatures.size() - 1) {
                return false;
            }
        }
        else {
            if (expectedArgumentSignatures.size() != actualArgumentTypes.size()) {
                return false;
            }
        }

        // Bind the variable arity argument first, to make sure it's bound to the common super type
        if (variableArity && actualArgumentTypes.size() >= expectedArgumentSignatures.size()) {
            List<? extends Type> variableArityParameterTypes = actualArgumentTypes.subList(
                    expectedArgumentSignatures.size() - 1, actualArgumentTypes.size()
            );
            Optional<Type> variableArityParameterActualType = typeManager.getCommonSuperType(variableArityParameterTypes);
            if (!variableArityParameterActualType.isPresent()) {
                return false;
            }
            TypeSignature expectedVariableArityParameterSignature = expectedArgumentSignatures.get(expectedArgumentSignatures.size() - 1);
            if (!matchAndBindArgument(expectedVariableArityParameterSignature, variableArityParameterActualType.get(), boundVariables)) {
                return false;
            }
        }

        for (int i = 0; i < actualArgumentTypes.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature expectedArgumentSignature = expectedArgumentSignatures.get(Math.min(i, expectedArgumentSignatures.size() - 1));
            Type actualArgumentType = actualArgumentTypes.get(i);
            if (!matchAndBindArgument(expectedArgumentSignature, actualArgumentType, boundVariables)) {
                return false;
            }
        }

        return true;
    }

    private boolean matchAndBindArgument(
            TypeSignature expectedArgumentSignature,
            Type actualArgumentType,
            BoundVariables.Builder boundVariables)
    {
        if (isTypeVariable(expectedArgumentSignature)) {
            String typeVariableName = expectedArgumentSignature.getBase();
            return matchAndBindTypeVariable(typeVariableName, actualArgumentType, boundVariables);
        }

        TypeSignature actualArgumentSignature = actualArgumentType.getTypeSignature();

        if (baseTypesAreEqual(expectedArgumentSignature, actualArgumentSignature)
                && matchAndBindTypeParameters(expectedArgumentSignature, actualArgumentType, boundVariables)) {
            return true;
        }
        else if (allowCoercion && canCoerce(actualArgumentSignature, expectedArgumentSignature)) {
            Type commonSuperType = commonSuperType(actualArgumentSignature, expectedArgumentSignature);
            checkState(baseTypesAreEqual(expectedArgumentSignature, commonSuperType.getTypeSignature()),
                    "base types are supposed to be equal after coercion");
            return matchAndBindTypeParameters(expectedArgumentSignature, commonSuperType, boundVariables);
        }

        return false;
    }

    private boolean matchAndBindTypeVariable(
            String typeVariableName,
            Type actualArgumentType,
            BoundVariables.Builder boundVariables)
    {
        TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(typeVariableName);
        if (!boundVariables.containsTypeVariable(typeVariableName)) {
            if (typeVariableConstraint.canBind(actualArgumentType)) {
                boundVariables.setTypeVariable(typeVariableName, actualArgumentType);
                return true;
            }
            if (allowCoercion && typeVariableConstraint.getVariadicBound() != null) {
                TypeSignature actualArgumentSignature = actualArgumentType.getTypeSignature();
                TypeSignature variadicBoundSignature = new TypeSignature(typeVariableConstraint.getVariadicBound());
                if (canCoerce(actualArgumentSignature, variadicBoundSignature)) {
                    Type commonType = commonSuperType(actualArgumentSignature, variadicBoundSignature);
                    boundVariables.setTypeVariable(typeVariableName, commonType);
                    return true;
                }
            }
        }
        // type variable is already bound. just check that bound types are compatible.
        else {
            Type currentBoundType = boundVariables.getTypeVariable(typeVariableName);
            if (currentBoundType.equals(actualArgumentType)) {
                return true;
            }
            if (allowCoercion) {
                Optional<Type> commonSuperType = typeManager.getCommonSuperType(currentBoundType, actualArgumentType);
                if (commonSuperType.isPresent() && typeVariableConstraint.canBind(commonSuperType.get())) {
                    boundVariables.setTypeVariable(typeVariableName, commonSuperType.get());
                    return true;
                }
            }
        }
        return false;
    }

    private boolean matchAndBindTypeParameters(
            TypeSignature expectedArgumentSignature,
            Type actualArgumentType,
            BoundVariables.Builder boundVariables)
    {
        TypeSignature actualArgumentSignature = actualArgumentType.getTypeSignature();
        checkState(baseTypesAreEqual(expectedArgumentSignature, actualArgumentSignature), "equal base types are expected here");

        List<TypeSignatureParameter> expectedTypeParameters = expectedArgumentSignature.getParameters();
        List<TypeSignatureParameter> actualTypeParameters = actualArgumentSignature.getParameters();

        // expected no parameters, but actual type has parameters bound
        // actually such situation may happen only for varchar
        // in future we should replace not parametrized VARCHAR type with the parametrized in function signatures
        if (expectedTypeParameters.isEmpty() && !actualTypeParameters.isEmpty()) {
            checkState(expectedArgumentSignature.getBase().equals(StandardTypes.VARCHAR),
                    "This is just legacy trick for VARCHAR. " +
                            "newly added methods for parametrized types must declare literal parameters in signatures");
            return true;
        }

        if (expectedTypeParameters.size() != actualTypeParameters.size()) {
            return false;
        }

        for (int typeParameterIndex = 0; typeParameterIndex < expectedTypeParameters.size(); typeParameterIndex++) {
            TypeSignatureParameter expectedTypeParameter = expectedTypeParameters.get(typeParameterIndex);
            TypeSignatureParameter actualTypeParameter = actualTypeParameters.get(typeParameterIndex);
            if (!matchAndBindTypeParameter(expectedTypeParameter, actualTypeParameter, boundVariables)) {
                return false;
            }
        }

        return true;
    }

    private boolean matchAndBindTypeParameter(
            TypeSignatureParameter expectedTypeParameter,
            TypeSignatureParameter actualTypeParameter,
            BoundVariables.Builder boundVariables)
    {
        switch (expectedTypeParameter.getKind()) {
            case VARIABLE: {
                String variableName = expectedTypeParameter.getVariable();
                checkState(actualTypeParameter.getKind() == ParameterKind.LONG,
                        "LONG parameter kind is expected here");
                Long variableValue = actualTypeParameter.getLongLiteral();
                return matchAndBindLongVariable(variableName, variableValue, boundVariables);
            }
            case LONG: {
                checkState(actualTypeParameter.getKind() == ParameterKind.LONG,
                        "LONG parameter kind is expected here");
                return actualTypeParameter.getLongLiteral().equals(expectedTypeParameter.getLongLiteral());
            }
            case TYPE: {
                checkState(actualTypeParameter.getKind() == ParameterKind.TYPE,
                        "TYPE parameter kind is expected here");
                TypeSignature expectedTypeSignature = expectedTypeParameter.getTypeSignature();
                TypeSignature actualTypeSignature = actualTypeParameter.getTypeSignature();
                Type actualType = typeManager.getType(actualTypeSignature);
                return matchAndBindArgument(expectedTypeSignature, actualType, boundVariables);
            }
            case NAMED_TYPE: {
                checkState(actualTypeParameter.getKind() == ParameterKind.NAMED_TYPE,
                        "NAMED_TYPE parameter kind is expected here");
                NamedTypeSignature expectedNamedTypeSignature = expectedTypeParameter.getNamedTypeSignature();
                NamedTypeSignature actualNamedTypeSignature = actualTypeParameter.getNamedTypeSignature();
                if (!expectedNamedTypeSignature.getName().equals(actualNamedTypeSignature.getName())) {
                    return false;
                }
                TypeSignature expectedTypeSignature = expectedNamedTypeSignature.getTypeSignature();
                TypeSignature actualTypeSignature = actualNamedTypeSignature.getTypeSignature();
                Type actualType = typeManager.getType(actualTypeSignature);
                return matchAndBindArgument(expectedTypeSignature, actualType, boundVariables);
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + expectedTypeParameter.getKind());
        }
    }

    private boolean matchAndBindLongVariable(
            String variableName,
            Long variableValue,
            BoundVariables.Builder boundVariables)
    {
        if (boundVariables.containsLongVariable(variableName)) {
            Long currentVariableValue = boundVariables.getLongVariable(variableName);
            return variableValue.equals(currentVariableValue);
        }
        else {
            boundVariables.setLongVariable(variableName, variableValue);
            return true;
        }
    }

    private void calculateVariableValuesForLongConstraints(BoundVariables.Builder boundVariables)
    {
        for (LongVariableConstraint longVariableConstraint : declaredSignature.getLongVariableConstraints()) {
            String calculation = longVariableConstraint.getExpression();
            String variableName = longVariableConstraint.getName();
            Long calculatedValue = calculateLiteralValue(calculation, boundVariables.getLongVariables());
            if (boundVariables.containsLongVariable(variableName)) {
                Long currentValue = boundVariables.getLongVariable(variableName);
                checkState(Objects.equals(currentValue, calculatedValue),
                        "variable '%s' is already set to %s when trying to set %s", variableName, currentValue, calculatedValue);
            }
            boundVariables.setLongVariable(variableName, calculatedValue);
        }
    }

    private boolean isTypeVariable(TypeSignature signature)
    {
        if (typeVariableConstraints.containsKey(signature.getBase())) {
            checkState(signature.getParameters().isEmpty(),
                    "TypeSignature that represent type variable shouldn't be parametrized");
            return true;
        }
        return false;
    }

    private Type commonSuperType(TypeSignature actualTypeSignature, TypeSignature requiredTypeSignature)
    {
        Optional<TypeSignature> commonSuperType = getCommonSuperTypeSignature(actualTypeSignature, requiredTypeSignature);
        checkState(commonSuperType.isPresent(), "common supper type signature hasn't been found");
        Type type = typeManager.getType(commonSuperType.get());
        checkState(type != null, "type signature for concrete type must be calculated here");
        return type;
    }

    private boolean baseTypesAreEqual(TypeSignature firstTypeSignature, TypeSignature secondTypeSignature)
    {
        return firstTypeSignature.getBase().equals(secondTypeSignature.getBase());
    }

    private void checkAllTypeVariablesAreBound(BoundVariables boundVariables)
    {
        Map<String, Type> typeVariableBindings = boundVariables.getTypeVariables();
        checkState(typeVariableBindings.keySet().equals(typeVariableConstraints.keySet()),
                "Type constraints %s are still unbound",
                Sets.difference(typeVariableConstraints.keySet(), typeVariableBindings.keySet()));
    }

    public static Signature bindVariables(Signature signature, BoundVariables boundVariables, int arity)
    {
        List<TypeSignature> argumentSignatures = fillInMissingVariableArguments(signature.getArgumentTypes(), arity);
        List<TypeSignature> boundArgumentSignatures = bindVariables(argumentSignatures, boundVariables);
        TypeSignature boundReturnTypeSignature = bindVariables(signature.getReturnType(), boundVariables);

        return new Signature(
                signature.getName(),
                signature.getKind(),
                ImmutableList.of(),
                ImmutableList.of(),
                boundReturnTypeSignature,
                boundArgumentSignatures,
                false
        );
    }

    private static List<TypeSignature> fillInMissingVariableArguments(List<TypeSignature> argumentSignatures, int arity)
    {
        int variableArityArgumentsCount = arity - argumentSignatures.size();
        if (variableArityArgumentsCount > 0 && !argumentSignatures.isEmpty()) {
            ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
            builder.addAll(argumentSignatures);
            for (int i = 0; i < variableArityArgumentsCount; i++) {
                builder.add(getLast(argumentSignatures));
            }
            return builder.build();
        }
        return argumentSignatures;
    }

    public static List<TypeSignature> bindVariables(List<TypeSignature> typeSignatures, BoundVariables boundVariables)
    {
        ImmutableList.Builder<TypeSignature> builder = ImmutableList.builder();
        for (TypeSignature typeSignature : typeSignatures) {
            builder.add(bindVariables(typeSignature, boundVariables));
        }
        return builder.build();
    }

    public static TypeSignature bindVariables(
            TypeSignature typeSignature,
            BoundVariables boundVariables)
    {
        String baseType = typeSignature.getBase();
        if (boundVariables.containsTypeVariable(baseType)) {
            checkState(typeSignature.getParameters().isEmpty(), "Type parameters cannot have parameters");
            return boundVariables.getTypeVariable(baseType).getTypeSignature();
        }

        List<TypeSignatureParameter> parameters = typeSignature.getParameters().stream()
                .map(typeSignatureParameter -> bindVariables(typeSignatureParameter, boundVariables))
                .collect(toList());

        return new TypeSignature(baseType, parameters);
    }

    private static TypeSignatureParameter bindVariables(
            TypeSignatureParameter typeSignatureParameter,
            BoundVariables boundVariables)
    {
        ParameterKind parameterKind = typeSignatureParameter.getKind();
        switch (parameterKind) {
            case TYPE: {
                TypeSignature typeSignature = typeSignatureParameter.getTypeSignature();
                return TypeSignatureParameter.of(bindVariables(typeSignature, boundVariables));
            }
            case NAMED_TYPE: {
                NamedTypeSignature namedTypeSignature = typeSignatureParameter.getNamedTypeSignature();
                TypeSignature typeSignature = namedTypeSignature.getTypeSignature();
                return TypeSignatureParameter.of(new NamedTypeSignature(
                        namedTypeSignature.getName(),
                        bindVariables(typeSignature, boundVariables)));
            }
            case VARIABLE: {
                String variableName = typeSignatureParameter.getVariable();
                checkState(boundVariables.containsLongVariable(variableName),
                        "Variable is not bound: %s", variableName);
                Long variableValue = boundVariables.getLongVariable(variableName);
                return TypeSignatureParameter.of(variableValue);
            }
            case LONG: {
                return typeSignatureParameter;
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + typeSignatureParameter.getKind());
        }
    }
}
