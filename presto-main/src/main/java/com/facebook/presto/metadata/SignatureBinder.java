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
import com.facebook.presto.type.UnknownType;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.type.TypeCalculation.calculateLiteralValue;
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

    public Optional<Signature> bind(List<? extends Type> actualArgumentTypes)
    {
        Optional<BoundVariables> boundVariables = bindVariables(actualArgumentTypes);
        if (!boundVariables.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(bindVariables(declaredSignature, boundVariables.get(), actualArgumentTypes.size()));
    }

    public Optional<BoundVariables> bindVariables(List<? extends Type> actualArgumentTypes)
    {
        List<TypeSignature> expectedArgumentSignatures = declaredSignature.getArgumentTypes();
        boolean variableArity = declaredSignature.isVariableArity();
        BoundVariables.Builder variableBinder = BoundVariables.builder();
        if (!matchArguments(expectedArgumentSignatures, actualArgumentTypes, variableArity, variableBinder)) {
            return Optional.empty();
        }
        calculateVariableValuesForLongConstraints(variableBinder);
        BoundVariables boundVariables = variableBinder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    public Optional<BoundVariables> bindVariables(List<? extends Type> actualArgumentTypes, Type actualReturnType)
    {
        BoundVariables.Builder variableBinder = BoundVariables.builder();

        TypeSignature expectedReturnTypeSignature = declaredSignature.getReturnType();
        if (!bind(expectedReturnTypeSignature, actualReturnType, variableBinder)) {
            return Optional.empty();
        }

        List<TypeSignature> expectedArgumentSignatures = declaredSignature.getArgumentTypes();
        boolean variableArity = declaredSignature.isVariableArity();
        if (!matchArguments(expectedArgumentSignatures, actualArgumentTypes, variableArity, variableBinder)) {
            return Optional.empty();
        }

        calculateVariableValuesForLongConstraints(variableBinder);
        BoundVariables boundVariables = variableBinder.build();
        if (!allTypeVariablesBound(boundVariables)) {
            return Optional.empty();
        }
        return Optional.of(boundVariables);
    }

    private boolean matchArguments(
            List<TypeSignature> expectedTypes,
            List<? extends Type> actualTypes,
            boolean variableArity,
            BoundVariables.Builder variableBinder)
    {
        if (variableArity && (actualTypes.size() < (expectedTypes.size() - 1))) {
            return false;
        }

        if (!variableArity && expectedTypes.size() != actualTypes.size()) {
            return false;
        }

        // Bind the variable arity argument first, to make sure it's bound to the common super type
        if (variableArity && actualTypes.size() >= expectedTypes.size()) {
            List<? extends Type> tail = actualTypes.subList(expectedTypes.size() - 1, actualTypes.size());
            Optional<Type> commonType = typeManager.getCommonSuperType(tail);
            if (!commonType.isPresent()) {
                return false;
            }
            TypeSignature expected = expectedTypes.get(expectedTypes.size() - 1);
            if (!bind(expected, commonType.get(), variableBinder)) {
                return false;
            }
        }

        for (int i = 0; i < actualTypes.size(); i++) {
            // Get the current argument signature, or the last one, if this is a varargs function
            TypeSignature expectedArgumentSignature = expectedTypes.get(Math.min(i, expectedTypes.size() - 1));
            Type actualArgumentType = actualTypes.get(i);
            if (!bind(expectedArgumentSignature, actualArgumentType, variableBinder)) {
                return false;
            }
        }

        return true;
    }

    private boolean bind(
            TypeSignature expectedSignature,
            Type actualType,
            BoundVariables.Builder variableBinder)
    {
        if (isTypeVariable(expectedSignature)) {
            String variable = expectedSignature.getBase();
            return matchAndBindTypeVariable(variable, actualType, variableBinder);
        }

        // TODO: Refactor VARCHAR function to accept parametrized VARCHAR(X) instead of VARCHAR and remove this hack
        if (expectedSignature.getBase().equals(StandardTypes.VARCHAR) && expectedSignature.getParameters().isEmpty()) {
            return actualType.getTypeSignature().getBase().equals(StandardTypes.VARCHAR) ||
                    (allowCoercion && typeManager.coerceTypeBase(actualType, StandardTypes.VARCHAR).isPresent());
        }

        // If the expected signature is a signature of the concrete type no variables are need to be bound
        // For such case it is enough to just check whether the actual type is coercible to the expected
        if (isConcreteType(expectedSignature)) {
            Type expectedType = typeManager.getType(expectedSignature);
            if (allowCoercion) {
                return typeManager.canCoerce(actualType, expectedType);
            }
            else {
                return actualType.equals(expectedType);
            }
        }

        String actualTypeBase = actualType.getTypeSignature().getBase();
        String expectedTypeBase = expectedSignature.getBase();
        if (actualTypeBase.equals(expectedTypeBase)) {
            return matchAndBindTypeParameters(expectedSignature, actualType, variableBinder);
        }
        else if (allowCoercion) {
            Optional<Type> coercionResult = typeManager.coerceTypeBase(actualType, expectedTypeBase);
            if (coercionResult.isPresent()) {
                return matchAndBindTypeParameters(expectedSignature, coercionResult.get(), variableBinder);
            }
            // UNKNOWN matches to all the types, but based on the UNKNOWN type parameters can't be bound
            if (actualType.equals(UnknownType.UNKNOWN)) {
                return true;
            }
        }

        return false;
    }

    private boolean matchAndBindTypeVariable(
            String variable,
            Type actualType,
            BoundVariables.Builder variableBinder)
    {
        TypeVariableConstraint typeVariableConstraint = typeVariableConstraints.get(variable);
        if (!variableBinder.containsTypeVariable(variable)) {
            if (typeVariableConstraint.canBind(actualType)) {
                variableBinder.setTypeVariable(variable, actualType);
                return true;
            }

            // UNKNOWN matches to all the types, but based on UNKNOWN we can't determine the actual type parameters
            if (allowCoercion && actualType.equals(UnknownType.UNKNOWN)) {
                return true;
            }

            // TODO Refactor type registry and support coercion to variadic bound fully
        }
        // type variable is already bound. just check that bound types are compatible.
        else {
            Type currentBoundType = variableBinder.getTypeVariable(variable);
            if (currentBoundType.equals(actualType)) {
                return true;
            }
            if (allowCoercion) {
                Optional<Type> commonSuperType = typeManager.getCommonSuperType(currentBoundType, actualType);
                if (commonSuperType.isPresent() && typeVariableConstraint.canBind(commonSuperType.get())) {
                    variableBinder.setTypeVariable(variable, commonSuperType.get());
                    return true;
                }
            }
        }
        return false;
    }

    private boolean matchAndBindTypeParameters(
            TypeSignature expectedArgumentSignature,
            Type actualArgumentType,
            BoundVariables.Builder variableBinder)
    {
        TypeSignature actualArgumentSignature = actualArgumentType.getTypeSignature();
        checkState(expectedArgumentSignature.getBase().equals(actualArgumentSignature.getBase()), "equal base types are expected here");

        List<TypeSignatureParameter> expectedTypeParameters = expectedArgumentSignature.getParameters();
        List<TypeSignatureParameter> actualTypeParameters = actualArgumentSignature.getParameters();

        if (expectedTypeParameters.size() != actualTypeParameters.size()) {
            return false;
        }

        for (int typeParameterIndex = 0; typeParameterIndex < expectedTypeParameters.size(); typeParameterIndex++) {
            TypeSignatureParameter expectedTypeParameter = expectedTypeParameters.get(typeParameterIndex);
            TypeSignatureParameter actualTypeParameter = actualTypeParameters.get(typeParameterIndex);
            if (!matchAndBindTypeParameter(expectedTypeParameter, actualTypeParameter, variableBinder)) {
                return false;
            }
        }

        return true;
    }

    private boolean matchAndBindTypeParameter(TypeSignatureParameter expected, TypeSignatureParameter actual, BoundVariables.Builder variableBinder)
    {
        switch (expected.getKind()) {
            case VARIABLE: {
                String variable = expected.getVariable();
                checkState(actual.getKind() == ParameterKind.LONG,
                        "LONG parameter kind is expected here");
                Long variableValue = actual.getLongLiteral();
                return matchAndBindLongVariable(variable, variableValue, variableBinder);
            }
            case LONG: {
                checkState(actual.getKind() == ParameterKind.LONG,
                        "LONG parameter kind is expected here");
                return actual.getLongLiteral().equals(expected.getLongLiteral());
            }
            case TYPE: {
                checkState(actual.getKind() == ParameterKind.TYPE,
                        "TYPE parameter kind is expected here");
                TypeSignature expectedTypeSignature = expected.getTypeSignature();
                TypeSignature actualTypeSignature = actual.getTypeSignature();
                Type actualType = typeManager.getType(actualTypeSignature);
                return bind(expectedTypeSignature, actualType, variableBinder);
            }
            case NAMED_TYPE: {
                checkState(actual.getKind() == ParameterKind.NAMED_TYPE,
                        "NAMED_TYPE parameter kind is expected here");
                NamedTypeSignature expectedNamedTypeSignature = expected.getNamedTypeSignature();
                NamedTypeSignature actualNamedTypeSignature = actual.getNamedTypeSignature();
                if (!expectedNamedTypeSignature.getName().equals(actualNamedTypeSignature.getName())) {
                    return false;
                }
                TypeSignature expectedTypeSignature = expectedNamedTypeSignature.getTypeSignature();
                TypeSignature actualTypeSignature = actualNamedTypeSignature.getTypeSignature();
                Type actualType = typeManager.getType(actualTypeSignature);
                return bind(expectedTypeSignature, actualType, variableBinder);
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + expected.getKind());
        }
    }

    private boolean matchAndBindLongVariable(String variable, Long value, BoundVariables.Builder variableBinder)
    {
        if (variableBinder.containsLongVariable(variable)) {
            Long currentVariableValue = variableBinder.getLongVariable(variable);
            return value.equals(currentVariableValue);
        }
        else {
            variableBinder.setLongVariable(variable, value);
            return true;
        }
    }

    private void calculateVariableValuesForLongConstraints(BoundVariables.Builder variableBinder)
    {
        for (LongVariableConstraint longVariableConstraint : declaredSignature.getLongVariableConstraints()) {
            String calculation = longVariableConstraint.getExpression();
            String variableName = longVariableConstraint.getName();
            Long calculatedValue = calculateLiteralValue(calculation, variableBinder.getLongVariables());
            if (variableBinder.containsLongVariable(variableName)) {
                Long currentValue = variableBinder.getLongVariable(variableName);
                checkState(Objects.equals(currentValue, calculatedValue),
                        "variable '%s' is already set to %s when trying to set %s", variableName, currentValue, calculatedValue);
            }
            variableBinder.setLongVariable(variableName, calculatedValue);
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

    private boolean isConcreteType(TypeSignature typeSignature)
    {
        if (isTypeVariable(typeSignature)) {
            return false;
        }
        for (TypeSignatureParameter typeSignatureParameter : typeSignature.getParameters()) {
            switch (typeSignatureParameter.getKind()) {
                case LONG:
                    continue;
                case VARIABLE:
                    return false;
                case TYPE: {
                    if (!isConcreteType(typeSignatureParameter.getTypeSignature())) {
                        return false;
                    }
                    continue;
                }
                case NAMED_TYPE: {
                    if (!isConcreteType(typeSignatureParameter.getNamedTypeSignature().getTypeSignature())) {
                        return false;
                    }
                    continue;
                }
                default:
                    throw new UnsupportedOperationException("Unsupported TypeSignatureParameter kind: " + typeSignatureParameter.getKind());
            }
        }
        return true;
    }

    private boolean allTypeVariablesBound(BoundVariables boundVariables)
    {
        return boundVariables.getTypeVariables().keySet().equals(typeVariableConstraints.keySet());
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
            TypeSignatureParameter parameter,
            BoundVariables boundVariables)
    {
        ParameterKind parameterKind = parameter.getKind();
        switch (parameterKind) {
            case TYPE: {
                TypeSignature typeSignature = parameter.getTypeSignature();
                return TypeSignatureParameter.of(bindVariables(typeSignature, boundVariables));
            }
            case NAMED_TYPE: {
                NamedTypeSignature namedTypeSignature = parameter.getNamedTypeSignature();
                TypeSignature typeSignature = namedTypeSignature.getTypeSignature();
                return TypeSignatureParameter.of(new NamedTypeSignature(
                        namedTypeSignature.getName(),
                        bindVariables(typeSignature, boundVariables)));
            }
            case VARIABLE: {
                String variableName = parameter.getVariable();
                checkState(boundVariables.containsLongVariable(variableName),
                        "Variable is not bound: %s", variableName);
                Long variableValue = boundVariables.getLongVariable(variableName);
                return TypeSignatureParameter.of(variableValue);
            }
            case LONG: {
                return parameter;
            }
            default:
                throw new IllegalStateException("Unknown parameter kind: " + parameter.getKind());
        }
    }
}
